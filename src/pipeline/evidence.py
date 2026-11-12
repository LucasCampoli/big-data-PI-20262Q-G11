"""Save row counts after a run.

python -m pipeline.evidence LABEL

Writes evidence/entrega-2/counts_LABEL.json. Two runs, labeled run_1 and run_2,
are how we show the second execution does not add rows.
"""
import argparse
import json
import logging
import sys
from datetime import datetime, timezone

from . import lake
from .config import Config
from .serving import TABLES, connect, connection_settings
from .spark import build_session

log = logging.getLogger("pipeline.evidence")


def cassandra_rows(cfg: Config) -> dict:
    if cfg.serving_target == "none":
        return {"status": "skipped"}
    try:
        settings = connection_settings(cfg)
        cluster, session = connect(settings)
    except Exception as exc:
        return {"status": f"unavailable: {exc}"}
    try:
        out = {"status": "ok", "target": settings["target"]}
        for table, _ in TABLES.values():
            row = session.execute(f"SELECT COUNT(*) FROM {settings['keyspace']}.{table}").one()
            out[table] = row[0]
        return out
    finally:
        cluster.shutdown()


def snapshot(spark, cfg: Config, label: str):
    tables = {}
    for zone, entity, path in lake.tables(cfg.datalake):
        if lake.exists(path):
            tables[f"{zone}/{entity}"] = lake.read(spark, path).count()
    data = {
        "label": label,
        "taken_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "lake_rows": tables,
        "cassandra_rows": cassandra_rows(cfg),
    }
    cfg.evidence.mkdir(parents=True, exist_ok=True)
    path = cfg.evidence / f"counts_{label}.json"
    path.write_text(json.dumps(data, indent=2) + "\n")
    log.info("wrote %s", path)
    return path


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("label")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cfg = Config.from_env()
    spark = build_session(cfg, "counts")
    try:
        snapshot(spark, cfg, args.label)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
