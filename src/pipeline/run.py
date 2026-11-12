"""Pipeline entry point: python -m pipeline.run [stage ...]

With no arguments it runs every stage in order. A stage that fails stops the run, is logged with
its traceback and leaves the run summary with status "failed"; the exit code is 1.
"""
import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone

from . import anomalies, bronze_batch, bronze_stream, gold, serving, silver
from .config import Config
from .errors import PipelineError
from .spark import build_session, check_java

log = logging.getLogger("pipeline")

EXPECTED_MASTERS = ["customers_orgs", "users", "resources", "support_tickets",
                    "marketing_touches", "nps_surveys", "billing_monthly"]


def stage_check(ctx, metrics):
    """Fail fast on the environment before any Spark job runs."""
    cfg = ctx.cfg
    metrics["java"] = check_java(cfg)
    missing = [m for m in EXPECTED_MASTERS if not (cfg.landing / f"{m}.csv").exists()]
    stream_dir = cfg.landing / "usage_events_stream"
    parts = sorted(stream_dir.glob(bronze_stream.EVENT_FILES))
    others = sorted(p.name for p in stream_dir.glob("*") if p.is_file() and p not in parts)
    metrics["landing"] = str(cfg.landing)
    metrics["master_files"] = len(EXPECTED_MASTERS) - len(missing)
    metrics["event_parts"] = len(parts)
    if others:
        # The stream reads only *.jsonl, so these do not break it; they are reported, not counted.
        metrics["ignored_files"] = len(others)
        log.warning("%s files in %s are not event parts and are ignored, e.g. %s", len(others), stream_dir, others[0])
    if missing or not parts:
        raise PipelineError(f"Landing at {cfg.landing} is incomplete: missing {missing or 'event parts'}. "
                            "See README, section on getting the dataset")
    metrics["spark"] = ctx.spark.version
    if not cfg.pii_hash_salt:
        raise PipelineError("PII_HASH_SALT is not set: run make setup or set it in config/pipeline.env")


STAGES = {
    "check": stage_check,
    "bronze-batch": bronze_batch.run,
    "bronze-stream": bronze_stream.run,
    "silver": silver.run,
    "gold": gold.run,
    "anomalies": anomalies.run,
    "serving": serving.run,
}
DEFAULT_ORDER = list(STAGES)


class Context:
    def __init__(self, cfg: Config, run_id: str, spark):
        self.cfg = cfg
        self.run_id = run_id
        self.spark = spark
        # One timestamp per run: every row a run writes carries the same ingest/processed time.
        self.run_ts = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)


def open_log(cfg: Config, run_id: str):
    cfg.logs.mkdir(parents=True, exist_ok=True)
    path = cfg.logs / f"{run_id}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=[logging.StreamHandler(), logging.FileHandler(path)],
        force=True,
    )
    # py4j logs every gateway call at INFO and hides the pipeline lines.
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("cassandra").setLevel(logging.WARNING)
    return path


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("stages", nargs="*", help=f"any of {', '.join(STAGES)}; default: all")
    args = parser.parse_args(argv)
    stages = DEFAULT_ORDER if not args.stages or args.stages == ["all"] else args.stages
    unknown = [s for s in stages if s not in STAGES]
    if unknown:
        parser.error(f"unknown stage(s): {unknown}")

    cfg = Config.from_env()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_file = open_log(cfg, run_id)
    summary = {
        "run_id": run_id,
        "started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "stages": [],
        "status": "failed",
        "config": cfg.masked(),
    }
    log.info("run %s, stages %s, log %s", run_id, stages, log_file)
    log.info("config %s", cfg.masked())

    spark = None
    status = "failed"
    try:
        spark = build_session(cfg, f"cloud-provider-analytics-{run_id}")
        ctx = Context(cfg, run_id, spark)
        for name in stages:
            metrics = {}
            entry = {"stage": name, "status": "running", "metrics": metrics}
            summary["stages"].append(entry)
            start = time.monotonic()
            log.info("stage %s: start", name)
            try:
                STAGES[name](ctx, metrics)
                entry["status"] = "ok"
            except Exception as exc:
                entry["status"] = "failed"
                entry["error"] = f"{type(exc).__name__}: {exc}"
                raise
            finally:
                entry["seconds"] = round(time.monotonic() - start, 2)
                log.info("stage %s: %s in %.1f s", name, entry["status"], entry["seconds"])
        status = "ok"
    except PipelineError as exc:
        log.error("run stopped: %s", exc)
    except Exception:
        log.exception("run stopped by an unexpected error")
    finally:
        if spark is not None:
            spark.stop()
        summary["status"] = status
        summary["finished_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        path = cfg.logs / f"{run_id}.json"
        path.write_text(json.dumps(summary, indent=2, default=str) + "\n")
        log.info("run %s finished with status %s, summary %s", run_id, status, path)
    return 0 if status == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
