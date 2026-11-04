"""Streaming to Bronze: parse failures split off before the dedupe, nothing lost to the watermark,
and the checkpoint makes a second run read nothing."""
import json

from pipeline import bronze_stream, lake

BASE = {"timestamp": "2025-07-20T10:00:00Z", "org_id": "org_a", "resource_id": "res_a",
        "service": "compute", "region": "us-east", "metric": "cpu_hours", "value": 1.5,
        "unit": "hours", "cost_usd_increment": 0.4, "schema_version": 2, "carbon_kg": 0.01}


def line(**fields):
    return json.dumps({**BASE, **fields}) + "\n"


def write_parts(landing):
    stream = landing / "usage_events_stream"
    stream.mkdir(parents=True)
    (stream / "part_0.jsonl").write_text(
        line(event_id="e1") + line(event_id="e1")                      # in-flight duplicate
        + line(event_id=None, value=1.0) + line(event_id=None, value=2.0)  # two distinct rows, no id
        + line(event_id="e2", timestamp="not-a-timestamp")             # wrong type
        + '{"event_id": "e3", "timestamp": "2025-07-\n')               # broken JSON
    (stream / "part_1.jsonl").write_text(
        line(event_id="e4") + '{"broken": \n' + '{"also broken"\n')


def test_stream_splits_parse_failures_before_the_dedupe(make_ctx, tmp_path):
    write_parts(tmp_path / "landing")
    ctx = make_ctx(stream_max_files_per_trigger=1)
    metrics = {}
    bronze_stream.run(ctx, metrics)

    bronze = lake.read(ctx.spark, ctx.cfg.table("bronze", "usage_events"))
    ids = sorted(str(r.event_id) for r in bronze.collect())
    # e1 once, both rows without an id survive, e4 from the second part.
    assert ids == ["None", "None", "e1", "e4"]
    assert {"ingest_ts", "ingest_date", "source_file"} <= set(bronze.columns)

    rejects = lake.read(ctx.spark, ctx.cfg.table("quarantine", "usage_events_parse"))
    rules = sorted(r.rule_name for r in rejects.collect())
    # Three broken lines; the dedupe did not collapse them even though none has an event_id.
    assert rules == ["parse_malformed_json"] * 3 + ["parse_type_mismatch"]

    summary = metrics[bronze_stream.BRONZE_QUERY]
    assert summary["rows_dropped_by_watermark"] == 0
    assert summary["batches_with_data"] == 2


def test_rerun_with_the_same_checkpoint_reads_nothing(make_ctx, tmp_path):
    write_parts(tmp_path / "landing")
    ctx = make_ctx()
    bronze_stream.run(ctx, {})
    metrics = {}
    bronze_stream.run(ctx, metrics)
    assert metrics[bronze_stream.BRONZE_QUERY]["input_rows"] == 0
    assert lake.read(ctx.spark, ctx.cfg.table("bronze", "usage_events")).count() == 4
