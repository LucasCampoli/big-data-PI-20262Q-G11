"""Silver end to end over the fixture Landing: every rejection lands in Quarantine with its rule,
and every repair is applied and flagged."""
import pytest

from pipeline import bronze_batch, bronze_stream, lake, silver
from pipeline.config import REPO_ROOT

FIXTURES = REPO_ROOT / "tests" / "fixtures" / "landing"


@pytest.fixture(scope="module")
def lake_ctx(spark, tmp_path_factory):
    import dataclasses
    from pipeline.config import Config
    from pipeline.run import Context
    root = tmp_path_factory.mktemp("silver")
    cfg = dataclasses.replace(Config.from_env(), landing=FIXTURES, datalake=root / "datalake",
                              logs=root / "logs", pii_hash_salt="test-salt")
    ctx = Context(cfg, "test-run", spark)
    metrics = {}
    for stage in (bronze_batch.run, bronze_stream.run, silver.run):
        stage(ctx, metrics)
    ctx.metrics = metrics
    return ctx


def table(ctx, zone, entity):
    return lake.read(ctx.spark, ctx.cfg.table(zone, entity))


def test_every_quarantine_table_has_its_rules(lake_ctx):
    rules = {}
    for entity in ("usage_events_parse", "masters_parse", "usage_events_rules", "masters_rules"):
        rules[entity] = sorted(r.rule_name for r in table(lake_ctx, "quarantine", entity).collect())
    assert rules["usage_events_parse"] == ["parse_malformed_json", "parse_type_mismatch"]
    assert rules["masters_parse"] == ["parse_error"]
    assert rules["masters_rules"] == ["resolved_before_created"]
    assert rules["usage_events_rules"] == sorted([
        "event_id_null", "timestamp_unparseable", "metric_unknown", "unit_contradicts_metric",
        "value_not_castable", "org_id_unknown", "resource_id_unknown", "resource_org_mismatch"])


def test_quarantine_keeps_the_raw_record(lake_ctx):
    row = table(lake_ctx, "quarantine", "usage_events_rules").filter("rule_name = 'value_not_castable'").first()
    assert '"value":"n/a"' in row.raw_record
    assert row.source_file == "events_fixture_0000.jsonl"


def test_silver_events_meet_the_consigna_rules(lake_ctx):
    events = table(lake_ctx, "silver", "usage_events")
    assert events.filter("event_id IS NULL").count() == 0
    assert events.count() == events.select("event_id").distinct().count()
    assert events.filter("value_double IS NOT NULL AND unit IS NULL").count() == 0
    assert events.filter("cost_anomaly_flag").count() == 1
    genai = events.filter("event_id = 'evt_fixture_genai'").first()
    assert (genai.requests, genai.cpu_hours, genai.genai_tokens) == (30.0, None, 4200)


def test_master_repairs_and_flags(lake_ctx):
    orgs = {r.org_id: r for r in table(lake_ctx, "silver", "customers_orgs").collect()}
    assert orgs["org_fixture01"].nps_score is None and orgs["org_fixture01"].nps_score_invalid

    tickets = {r.ticket_id: r for r in table(lake_ctx, "silver", "support_tickets").collect()}
    assert set(tickets) == {"tkt_fixture01", "tkt_fixture03"}
    assert tickets["tkt_fixture03"].csat is None and tickets["tkt_fixture03"].csat_invalid
    assert tickets["tkt_fixture03"].is_open

    bills = {r.invoice_id: r for r in table(lake_ctx, "silver", "billing_monthly").collect()}
    usd = bills["inv_fixture01"]
    assert usd.fx_overridden and usd.exchange_rate_applied == 1.0
    assert usd.credits == 0.0 and usd.credits_imputed
    assert usd.revenue_usd == pytest.approx(121.0)
    eur = bills["inv_fixture02"]
    assert eur.subtotal_negative and eur.revenue_usd == pytest.approx((-50.0 - 5.0) * 1.1)

    users = table(lake_ctx, "silver", "users")
    assert "email" not in users.columns
    u1 = users.filter("user_id = 'user_fixture01'").first()
    assert u1.email_domain == "example.com" and u1.last_login_before_created
    assert users.filter("user_id = 'user_fixture02'").first().created_before_org_signup

    touch = table(lake_ctx, "silver", "marketing_touches").filter("touch_id = 'mkt_fixture01'").first()
    assert touch.converted_without_click


def test_rebuild_does_not_duplicate_silver_or_quarantine(lake_ctx):
    before = {e: table(lake_ctx, z, e).count() for z, e in
              [("silver", "usage_events"), ("quarantine", "usage_events_rules"), ("quarantine", "masters_rules")]}
    silver.run(lake_ctx, {})
    after = {e: table(lake_ctx, z, e).count() for z, e in
             [("silver", "usage_events"), ("quarantine", "usage_events_rules"), ("quarantine", "masters_rules")]}
    assert before == after


def test_silver_refuses_to_run_without_a_hash_salt(lake_ctx):
    import dataclasses
    from pipeline.errors import PipelineError
    from pipeline.run import Context
    unsalted = Context(dataclasses.replace(lake_ctx.cfg, pii_hash_salt=""), "test-run", lake_ctx.spark)
    with pytest.raises(PipelineError, match="PII_HASH_SALT"):
        silver.run(unsalted, {})
