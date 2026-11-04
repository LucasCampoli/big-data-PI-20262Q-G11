"""Structured Streaming from Landing usage_events_stream/*.jsonl into Bronze and Quarantine.

Two queries over the same directory source, each with its own queryName and checkpoint (design
§6.3): `bronze_usage_events` appends parsed, deduplicated events partitioned by ingest_date, and
`quarantine_usage_events_parse` appends the lines that did not parse.

Order matters. Parse failures are split off before the dedupe, because they arrive with a null
event_id and the dedupe would collapse them into one row. The watermark is on ingest_ts, stamped at
read time, so no event is ever late against it and Bronze keeps every event; it only bounds how long
an event_id is remembered for in-flight duplicates (D8). The exact dedupe happens in the Silver
rebuild.

Exactly-once into Bronze comes from a replayable source (files in a directory), the checkpoint
(offsets and the file sink commit log) and an idempotent file sink. A rerun with the same
checkpoint reads nothing new.
"""
import json
import logging

from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from . import quarantine
from .errors import PipelineError
from .schemas import CORRUPT_RECORD, USAGE_EVENT_SCHEMA, with_corrupt_column

log = logging.getLogger("pipeline.bronze_stream")

BRONZE_QUERY = "bronze_usage_events"
QUARANTINE_QUERY = "quarantine_usage_events_parse"
SOURCE_COLUMNS = [f.name for f in USAGE_EVENT_SCHEMA.fields]


class ProgressLogger(StreamingQueryListener):
    """Logs each micro-batch as it completes: input rows, duration, rows held in the dedupe state
    and rows dropped by the watermark, the empirical side of D8."""

    def onQueryStarted(self, event):
        log.info("query %s started (id %s)", event.name, event.id)

    def onQueryProgress(self, event):
        p = json.loads(event.progress.json)
        state = p.get("stateOperators") or [{}]
        log.info("query %s batch %s: %s input rows in %s ms, state rows %s, dropped by watermark %s",
                 p.get("name"), p.get("batchId"), p.get("numInputRows"),
                 (p.get("durationMs") or {}).get("triggerExecution"),
                 state[0].get("numRowsTotal", "-"), state[0].get("numRowsDroppedByWatermark", "-"))

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        if event.exception:
            log.error("query %s terminated with %s", event.id, event.exception)


def read_events(spark, cfg):
    return (spark.readStream
            .schema(with_corrupt_column(USAGE_EVENT_SCHEMA))
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_RECORD)
            .option("maxFilesPerTrigger", cfg.stream_max_files_per_trigger)
            .json(str(cfg.landing / "usage_events_stream"))
            .withColumn("source_file", F.col("_metadata.file_name"))
            .withColumn("ingest_ts", F.current_timestamp())
            .withColumn("ingest_date", F.to_date("ingest_ts")))


def dedupe_key():
    # A parseable row with a null event_id is still a distinct event until Silver rejects it, so it
    # is keyed by its content: identical copies collapse, different rows survive.
    content = F.sha2(F.to_json(F.struct(*SOURCE_COLUMNS)), 256)
    return F.coalesce(F.col("event_id"), F.concat(F.lit("no-id:"), content))


def bronze_frame(events, watermark: str):
    return (events
            .filter(F.col(CORRUPT_RECORD).isNull())
            .drop(CORRUPT_RECORD)
            .withColumn("_dedupe_key", dedupe_key())
            .withWatermark("ingest_ts", watermark)
            .dropDuplicatesWithinWatermark(["_dedupe_key"])
            .drop("_dedupe_key")
            # The dedupe state is split in shuffle partitions; without this each micro-batch
            # would write one small file per state partition instead of one file (design §5.1).
            .coalesce(1))


def quarantine_frame(events, run_id: str):
    # Spark keeps the fields it could convert, so a line where nothing parsed is broken JSON and a
    # line with some fields set has a value of the wrong type. Reading the fields also keeps the
    # JSON source from pruning the scan to the corrupt-record column, which it refuses to do.
    nothing_parsed = F.coalesce(*[F.col(c).cast("string") for c in SOURCE_COLUMNS]).isNull()
    rule = F.when(nothing_parsed, F.lit("parse_malformed_json")).otherwise(F.lit("parse_type_mismatch"))
    bad = events.filter(F.col(CORRUPT_RECORD).isNotNull())
    return quarantine.shape(bad, "usage_events", rule, F.col(CORRUPT_RECORD), run_id, None)


def apply_trigger(writer, cfg):
    if cfg.stream_trigger == "available_now":
        return writer.trigger(availableNow=True)
    if cfg.stream_trigger == "processing_time":
        return writer.trigger(processingTime=cfg.stream_processing_time)
    raise PipelineError(f"STREAM_TRIGGER must be available_now or processing_time, not {cfg.stream_trigger!r}")


def start_queries(ctx):
    cfg, spark = ctx.cfg, ctx.spark
    source = cfg.landing / "usage_events_stream"
    if not source.exists():
        raise PipelineError(f"streaming source {source} does not exist")
    events = read_events(spark, cfg)

    bronze = apply_trigger(
        bronze_frame(events, cfg.stream_watermark).writeStream
        .queryName(BRONZE_QUERY)
        .format("parquet")
        .outputMode("append")
        .partitionBy("ingest_date")
        .option("checkpointLocation", str(cfg.checkpoint(BRONZE_QUERY))), cfg
    ).start(str(cfg.table("bronze", "usage_events")))

    rejects = apply_trigger(
        quarantine_frame(events, ctx.run_id).writeStream
        .queryName(QUARANTINE_QUERY)
        .format("parquet")
        .outputMode("append")
        .partitionBy("quarantine_date")
        .option("checkpointLocation", str(cfg.checkpoint(QUARANTINE_QUERY))), cfg
    ).start(str(cfg.table("quarantine", "usage_events_parse")))
    return [bronze, rejects]


def summarize(progress: list) -> dict:
    out = {}
    for name in (BRONZE_QUERY, QUARANTINE_QUERY):
        batches = [p for p in progress if p.get("name") == name]
        state = [(p.get("stateOperators") or [{}])[0] for p in batches]
        out[name] = {
            "batches": len(batches),
            "batches_with_data": sum(1 for p in batches if p.get("numInputRows", 0) > 0),
            "input_rows": sum(p.get("numInputRows", 0) for p in batches),
            "rows_dropped_by_watermark": sum(s.get("numRowsDroppedByWatermark", 0) for s in state),
            "max_state_rows": max((s.get("numRowsTotal", 0) for s in state), default=0),
            "state_rows_removed": sum(s.get("numRowsRemoved", 0) for s in state),
        }
    return out


def progress_of(query) -> list:
    # recentProgress is complete once the query has stopped; the listener can still be delivering
    # the last events at that point, so it only drives the live log lines.
    return [json.loads(p.json) if hasattr(p, "json") else dict(p) for p in query.recentProgress]


def run(ctx, metrics):
    cfg = ctx.cfg
    recorder = ProgressLogger()
    ctx.spark.streams.addListener(recorder)
    try:
        queries = start_queries(ctx)
        if cfg.stream_trigger == "processing_time":
            log.info("continuous mode, trigger %s: Ctrl+C stops the queries", cfg.stream_processing_time)
        try:
            for q in queries:
                q.awaitTermination()
        except Exception as exc:
            for q in queries:
                if q.isActive:
                    q.stop()
            failed = [q.name for q in queries if q.exception()]
            raise PipelineError(f"streaming query {failed or ''} failed: {str(exc).splitlines()[0]}") from exc
    finally:
        ctx.spark.streams.removeListener(recorder)

    progress = [p for q in queries for p in progress_of(q)]
    cfg.logs.mkdir(parents=True, exist_ok=True)
    progress_file = cfg.logs / f"{ctx.run_id}_stream_progress.jsonl"
    progress_file.write_text("".join(json.dumps(p) + "\n" for p in progress))
    metrics.update(summarize(progress))
    metrics["progress_file"] = progress_file.name
    log.info("streaming summary %s", metrics)
