"""Run the serving queries against the configured target and keep their output.

python -m pipeline.queries [--out DIR]

The statements are read from cql/q*.cql, the same files cqlsh runs, so the evidence and the scripts
cannot drift apart. Q2's sum per service and ranking happen here, in the client (see the file).
"""
import argparse
import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from .config import Config
from .serving import CQL_DIR, connect, connection_settings, cql_statements

log = logging.getLogger("pipeline.queries")
TOP_N = 3


def table(rows, columns) -> str:
    lines = [" | ".join(columns)]
    for row in rows:
        cells = []
        for column in columns:
            value = row[column]
            if value is None:
                cells.append("")
            elif isinstance(value, float):
                cells.append(f"{value:.4f}")
            else:
                cells.append(str(value))
        lines.append(" | ".join(cells))
    return "\n".join(lines)


def run_file(session, name: str, keyspace: str):
    stmt = cql_statements(CQL_DIR / name, keyspace)[0]
    rows = [r._asdict() for r in session.execute(stmt)]
    return stmt, rows


def report(title: str, settings: dict, stmt: str, body: str) -> str:
    when = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return (f"{title}\ntarget: {settings['target']}  keyspace: {settings['keyspace']}  "
            f"consistency: {settings['consistency']}  run at: {when}\n\n{stmt};\n\n{body}\n")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", help="directory for the output files (default: EVIDENCE_DIR)")
    parser.add_argument("--keyspaces", action="store_true",
                        help="only list the keyspaces of the target (on AstraDB they come from the console)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("cassandra").setLevel(logging.WARNING)
    cfg = Config.from_env()
    out = Path(args.out) if args.out else cfg.evidence
    out.mkdir(parents=True, exist_ok=True)
    try:
        settings = connection_settings(cfg)
        cluster, session = connect(settings)
    except Exception as exc:
        log.error("cannot connect: %s", exc)
        return 1
    if args.keyspaces:
        for r in session.execute("SELECT keyspace_name FROM system_schema.keyspaces"):
            print(r.keyspace_name)
        cluster.shutdown()
        return 0
    suffix = "" if settings["target"] == "cassandra" else f"_{settings['target']}"
    try:
        stmt, rows = run_file(session, "q1_daily_cost_by_service.cql", settings["keyspace"])
        q1 = report("Q1. Daily cost and requests per service, one organization, date range",
                    settings, stmt, table(rows, ["usage_date", "service", "daily_cost_usd", "requests", "events"])
                    + f"\n\n({len(rows)} rows)")

        stmt, rows = run_file(session, "q2_top_services_14d.cql", settings["keyspace"])
        cost, days = defaultdict(float), defaultdict(int)
        for r in rows:
            cost[r["service"]] += r["daily_cost_usd"] or 0.0
            days[r["service"]] += 1
        ranking = sorted(cost, key=cost.get, reverse=True)
        ranked = [{"rank": i + 1, "service": s, "cost_14d_usd": round(cost[s], 4), "days_with_usage": days[s]}
                  for i, s in enumerate(ranking)]
        q2 = report(f"Q2. Top {TOP_N} services by cost over the last 14 days, one organization",
                    settings, stmt,
                    f"{len(rows)} rows read from one partition, summed and ranked in the client.\n\n"
                    + table(ranked[:TOP_N], ["rank", "service", "cost_14d_usd", "days_with_usage"])
                    + "\n\nfull ranking:\n" + table(ranked, ["rank", "service", "cost_14d_usd", "days_with_usage"]))

        stmt, rows = run_file(session, "q6_cost_anomalies.cql", settings["keyspace"])
        flagged = [r for r in rows if r["is_anomaly"]]
        q6 = report("Q6. Days flagged as cost anomalies, one organization, August", settings, stmt,
                    f"{len(rows)} scored rows read, {len(flagged)} with is_anomaly = true:\n\n"
                    + table(flagged, ["usage_date", "service", "daily_cost_usd", "median_cost",
                                      "anomaly_score", "anomaly_direction"]))
    except Exception as exc:
        log.error("query failed: %s", exc)
        return 1
    finally:
        cluster.shutdown()

    for name, text in (("q1", q1), ("q2", q2), ("q6", q6)):
        path = out / f"serving_{name}_driver{suffix}.txt"
        path.write_text(text)
        print(text)
        log.info("wrote %s", path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
