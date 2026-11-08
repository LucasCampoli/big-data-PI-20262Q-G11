"""Batch load of the six CSV masters and billing from Landing into Bronze.

Bronze only checks that a row parses with the declared schema (design §5). Rows that do not parse
go to quarantine/masters_parse with the raw CSV line. Re-running is safe because a file already in
the ingest registry is skipped (D8): Bronze is append-only and the registry is what keeps it free of
re-ingested copies.
"""
import hashlib
import logging

from pyspark.sql import functions as F

from . import lake, quarantine
from .errors import PipelineError
from .schemas import CORRUPT_RECORD, MASTER_KEYS, MASTER_PARTITION, MASTER_SCHEMAS, with_corrupt_column

log = logging.getLogger("pipeline.bronze_batch")


def sha256(path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_registry(spark, cfg) -> dict:
    if not lake.exists(cfg.registry):
        return {}
    return {r.file_name: r.sha256 for r in lake.read(spark, cfg.registry).select("file_name", "sha256").collect()}


def read_csv(spark, path, schema):
    return (spark.read.schema(with_corrupt_column(schema))
            .options(header=True,
                     # Compare the file header with the schema instead of trusting column order.
                     enforceSchema=False,
                     # tags_json comes quoted with doubled quotes inside.
                     escape='"',
                     mode="PERMISSIVE",
                     columnNameOfCorruptRecord=CORRUPT_RECORD,
                     dateFormat="yyyy-MM-dd")
            .csv(str(path)))


def ingest_master(ctx, entity: str, registered: dict) -> dict:
    cfg, spark = ctx.cfg, ctx.spark
    path = cfg.landing / f"{entity}.csv"
    digest = sha256(path)
    if path.name in registered:
        if registered[path.name] != digest:
            # Landing is immutable (D8): same name with new content is a contract break, not a
            # new snapshot. It is reported and left for a person to decide.
            log.warning("%s changed after it was ingested (sha256 differs); skipped", path.name)
        else:
            log.info("%s already in the ingest registry; skipped", path.name)
        return {"status": "skipped_registered"}

    df = (read_csv(spark, path, MASTER_SCHEMAS[entity])
          .withColumn("source_file", F.col("_metadata.file_name"))
          .withColumn("ingest_ts", F.lit(ctx.run_ts).cast("timestamp"))
          .withColumn("ingest_date", F.to_date("ingest_ts")))
    try:
        df = df.cache()
        rows = df.count()
    except Exception as exc:  # header mismatch, unreadable file
        raise PipelineError(f"cannot read {path.name}: {str(exc).splitlines()[0]}") from exc

    good = df.filter(F.col(CORRUPT_RECORD).isNull()).drop(CORRUPT_RECORD)
    bad = df.filter(F.col(CORRUPT_RECORD).isNotNull())
    target = cfg.table("bronze", entity)
    good.coalesce(1).write.mode("append").partitionBy(MASTER_PARTITION[entity]).parquet(str(target))

    rejected = bad.count()
    if rejected:
        q = quarantine.shape(bad, entity, F.lit("parse_error"), F.col(CORRUPT_RECORD), ctx.run_id, ctx.run_ts)
        q.write.mode("append").partitionBy("quarantine_date").parquet(str(cfg.table("quarantine", "masters_parse")))

    keys = MASTER_KEYS[entity]
    duplicate_keys = good.groupBy(*keys).count().filter("count > 1").count()
    df.unpersist()
    stats = {"status": "ingested", "rows_read": rows, "rows_bronze": rows - rejected,
             "rows_quarantined": rejected, "duplicate_keys": duplicate_keys,
             "size_bytes": path.stat().st_size, "sha256": digest}
    log.info("%s: %s rows to Bronze, %s to Quarantine, %s duplicate keys", path.name,
             stats["rows_bronze"], rejected, duplicate_keys)
    return stats


def register(ctx, entries: list):
    rows = [(name, entity, s["size_bytes"], s["sha256"], s["rows_read"], s["rows_bronze"],
             s["rows_quarantined"], ctx.run_id) for name, entity, s in entries]
    if not rows:
        return
    df = (ctx.spark.createDataFrame(
              rows, "file_name string, entity string, size_bytes long, sha256 string, rows_read long, "
                    "rows_bronze long, rows_quarantined long, run_id string")
          .withColumn("ingest_ts", F.lit(ctx.run_ts).cast("timestamp")))
    df.coalesce(1).write.mode("append").parquet(str(ctx.cfg.registry))


def run(ctx, metrics):
    registered = read_registry(ctx.spark, ctx.cfg)
    entries = []
    for entity in MASTER_SCHEMAS:
        stats = ingest_master(ctx, entity, registered)
        metrics[entity] = stats
        if stats["status"] == "ingested":
            entries.append((f"{entity}.csv", entity, stats))
    # The registry is written last: a crash before this line re-ingests the files on the next run,
    # and the Silver dedupe by natural key absorbs that copy.
    register(ctx, entries)
    metrics["files_ingested"] = len(entries)
    metrics["files_skipped"] = len(MASTER_SCHEMAS) - len(entries)
