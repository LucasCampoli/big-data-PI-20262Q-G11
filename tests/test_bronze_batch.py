"""Batch load to Bronze: parse failures go to Quarantine, the registry makes re-runs a no-op."""
import pytest

from pipeline import bronze_batch, lake
from pipeline.errors import PipelineError

HEADER = ("org_id,org_name,industry,hq_region,plan_tier,is_enterprise,signup_date,sales_rep,"
          "lifecycle_stage,marketing_source,nps_score\n")
GOOD = "org_a,Alpha,Energy,us-east,pro,True,2025-05-04,rep_a,active,ads,10.0\n"
BAD = "org_b,Beta,Energy,us-east,pro,maybe,2025-13-45,rep_a,active,ads,ten\n"


def write_orgs(landing, body):
    landing.mkdir(parents=True, exist_ok=True)
    (landing / "customers_orgs.csv").write_text(body)


def test_unparseable_row_is_quarantined_with_its_raw_line(make_ctx, tmp_path):
    write_orgs(tmp_path / "landing", HEADER + GOOD + BAD)
    ctx = make_ctx()
    stats = bronze_batch.ingest_master(ctx, "customers_orgs", {})
    assert (stats["rows_bronze"], stats["rows_quarantined"]) == (1, 1)

    target = ctx.cfg.table("bronze", "customers_orgs")
    bronze = lake.read(ctx.spark, target)
    assert [r.org_id for r in bronze.collect()] == ["org_a"]
    assert {"ingest_ts", "ingest_date", "source_file"} <= set(bronze.columns)
    # Each snapshot lands under the date it was loaded (D5).
    assert [d.name for d in target.iterdir() if d.is_dir()] == [f"ingest_date={ctx.run_ts.date()}"]

    q = lake.read(ctx.spark, ctx.cfg.table("quarantine", "masters_parse")).first()
    assert q.rule_name == "parse_error"
    assert q.raw_record == BAD.strip()
    assert q.source_file == "customers_orgs.csv"


def test_registered_file_is_skipped(make_ctx, tmp_path):
    write_orgs(tmp_path / "landing", HEADER + GOOD)
    ctx = make_ctx()
    first = bronze_batch.ingest_master(ctx, "customers_orgs", {})
    bronze_batch.register(ctx, [("customers_orgs.csv", "customers_orgs", first)])
    registered = bronze_batch.read_registry(ctx.spark, ctx.cfg)
    assert bronze_batch.ingest_master(ctx, "customers_orgs", registered)["status"] == "skipped_registered"
    assert lake.read(ctx.spark, ctx.cfg.table("bronze", "customers_orgs")).count() == 1


def test_header_that_does_not_match_the_schema_stops_the_load(make_ctx, tmp_path):
    swapped = HEADER.replace("org_id,org_name", "org_name,org_id")
    write_orgs(tmp_path / "landing", swapped + GOOD)
    with pytest.raises(PipelineError, match="cannot read customers_orgs.csv"):
        bronze_batch.ingest_master(make_ctx(), "customers_orgs", {})
