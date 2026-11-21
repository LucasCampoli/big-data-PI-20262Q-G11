"""Health of the pipeline, read from the run summaries in logs/ (observability, D20).

python -m pipeline.health [--out FILE]      (make health)

Each check is OK, WARN or ALERT. The exit code is 1 when any check is ALERT, so the scheduler that
runs make run (cron or Airflow in production) can run make health after it and notify on failure.
The report also keeps the history of the last runs: rows, Quarantine and time, run by run.
"""
import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import Config

RUN_FILE = re.compile(r"^\d{8}T\d{6}Z\.json$")
STALE_HOURS = 26    # Gold is D+1 (O1): a day without a full run, plus two hours of slack
HISTORY = 10


def summaries(logs: Path) -> list[dict]:
    """Run summaries, oldest first. The run_id is a UTC timestamp, so the name sorts by time."""
    files = sorted(p for p in logs.glob("*.json") if RUN_FILE.match(p.name))
    return [json.loads(p.read_text()) for p in files]


def stage(run: dict, name: str):
    return next((s for s in run.get("stages", []) if s["stage"] == name), None)


def last_with(runs: list[dict], name: str):
    """The latest run that ran a stage to the end, with that stage's entry."""
    for run in reversed(runs):
        entry = stage(run, name)
        if entry and entry["status"] == "ok":
            return run, entry
    return None, None


def quarantined(run: dict):
    """Rows sent to Quarantine by one run: CSV parse failures plus Silver rule violations."""
    silver = stage(run, "silver")
    if not silver or silver["status"] != "ok":
        return None
    batch = stage(run, "bronze-batch")
    parse = sum(m.get("rows_quarantined", 0) for m in (batch or {}).get("metrics", {}).values() if isinstance(m, dict))
    rules = sum(m.get("quarantined", 0) for k, m in silver["metrics"].items() if k != "usage_events")
    return parse + rules + silver["metrics"]["usage_events"]["quarantined_rows"]


def checks(runs: list[dict], now: datetime) -> list[tuple]:
    if not runs:
        return [("ALERT", "runs", "no run summary in the log directory: run make run")]
    out = []
    last = runs[-1]
    failed = [s for s in last["stages"] if s["status"] != "ok"]
    out.append(("ALERT", "last run", f"{last['run_id']} failed at {failed[0]['stage']}: {failed[0].get('error', '?')}")
               if failed or last["status"] != "ok" else ("OK", "last run", f"{last['run_id']} ok"))

    run, _ = last_with(runs, "gold")
    if run is None:
        out.append(("ALERT", "freshness", "no run has built Gold"))
    else:
        age = now - datetime.fromisoformat(run["finished_utc"])
        hours = age.total_seconds() / 3600
        out.append(("WARN" if age > timedelta(hours=STALE_HOURS) else "OK", "freshness",
                    f"Gold built by {run['run_id']}, {hours:.1f} h ago (limit {STALE_HOURS} h)"))

    _, s = last_with(runs, "bronze-stream")
    if s:
        dropped = s["metrics"]["bronze_usage_events"]["rows_dropped_by_watermark"]
        out.append(("ALERT" if dropped else "OK", "stream watermark",
                    f"{dropped} rows dropped by the watermark (D8 expects 0)"))

    _, s = last_with(runs, "silver")
    if s:
        ev = s["metrics"]["usage_events"]
        out.append(("ALERT" if ev["o3_breached"] else "OK", "O3 quarantine share",
                    f"{ev['quarantined_rows_pct']}% of events, {ev['quarantined_cost_pct']}% of cost (limit 1%)"))

    for name, label in (("gold", "Gold reconciliation"), ("marts", "marts reconciliation")):
        _, s = last_with(runs, name)
        if s:
            bad = [k for k, m in s["metrics"].items() if isinstance(m, dict) and not m.get("passed", True)]
            out.append(("ALERT" if bad else "OK", label,
                        f"failed: {', '.join(bad)}" if bad else f"{len(s['metrics'])} mart(s) passed"))

    _, s = last_with(runs, "serving")
    if s and s["metrics"].get("skipped"):
        out.append(("OK", "serving", "skipped (SERVING_TARGET=none)"))
    elif s:
        bad = [t for t, m in s["metrics"].items() if isinstance(m, dict) and m["rows_written"] != m["gold_rows"]]
        tables = [t for t, m in s["metrics"].items() if isinstance(m, dict)]
        out.append(("ALERT" if bad else "OK", "serving row check",
                    f"differs from Gold: {', '.join(bad)}" if bad else
                    f"{len(tables)} tables on {s['metrics'].get('target')}, rows equal to Gold"))

    counts = [(r["run_id"], quarantined(r)) for r in runs if quarantined(r) is not None]
    if len(counts) >= 2:
        (prev_id, prev), (last_id, now_q) = counts[-2], counts[-1]
        out.append(("WARN" if now_q > prev else "OK", "Quarantine growth",
                    f"{now_q} rows in {last_id}, {prev} in {prev_id}"))
    return out


def history(runs: list[dict]) -> list[dict]:
    rows = []
    for run in runs[-HISTORY:]:
        silver, gold, serving = stage(run, "silver"), stage(run, "gold"), stage(run, "serving")
        rows.append({
            "run_id": run["run_id"],
            "status": run["status"],
            "stages": len(run["stages"]),
            "silver_events": silver["metrics"]["usage_events"]["silver_rows"] if silver and silver["status"] == "ok" else "",
            "quarantined": quarantined(run) if quarantined(run) is not None else "",
            "gold_rows": gold["metrics"]["org_daily_usage_by_service"]["gold_rows"] if gold and gold["status"] == "ok" else "",
            "serving": (serving["metrics"].get("target") or "skipped") if serving and serving["status"] == "ok" else "",
            "seconds": round(sum(s.get("seconds", 0) for s in run["stages"]), 1),
        })
    return rows


def md_table(rows: list[dict]) -> str:
    if not rows:
        return "(none)\n"
    cols = list(rows[0])
    lines = ["| " + " | ".join(cols) + " |", "|" + " --- |" * len(cols)]
    lines += ["| " + " | ".join(str(r[c]) for c in cols) + " |" for r in rows]
    return "\n".join(lines) + "\n"


# A failed AstraDB connection names the database host; the report can end up in the evidence.
ASTRA_HOST = re.compile(r"[0-9a-f]{8}-[0-9a-f-]{27}[\w-]*\.db\.astra\.datastax\.com(:\d+)?")


def render(results: list[tuple], runs: list[dict], now: datetime) -> str:
    worst = "ALERT" if any(r[0] == "ALERT" for r in results) else "WARN" if any(r[0] == "WARN" for r in results) else "OK"
    return (f"# Pipeline health\n\nChecked at {now.isoformat(timespec='seconds')} over {len(runs)} run summaries. "
            f"Overall: **{worst}**.\n\n"
            + md_table([{"level": lvl, "check": name, "detail": detail} for lvl, name, detail in results])
            + f"\n## Last {min(HISTORY, len(runs))} runs\n\n" + md_table(history(runs)))


def masked(text: str) -> str:
    return ASTRA_HOST.sub("<astra-host>", text)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", help="also write the report to this file")
    args = parser.parse_args(argv)
    cfg = Config.from_env()
    runs = summaries(cfg.logs)
    now = datetime.now(timezone.utc)
    results = checks(runs, now)
    text = masked(render(results, runs, now))
    print(text)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text)
    return 1 if any(level == "ALERT" for level, _, _ in results) else 0


if __name__ == "__main__":
    sys.exit(main())
