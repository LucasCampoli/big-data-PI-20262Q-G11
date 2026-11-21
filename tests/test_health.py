"""Health checks over run summaries: what is an alert, what is a warning."""
import json
from datetime import datetime, timedelta, timezone

from pipeline import health

NOW = datetime(2026, 12, 1, 12, tzinfo=timezone.utc)


def run(run_id, hours_ago=1, status="ok", quarantined=0, dropped=0, written=10, failed_stage=None):
    stages = [
        {"stage": "bronze-batch", "status": "ok", "metrics": {"users": {"rows_quarantined": 0}}},
        {"stage": "bronze-stream", "status": "ok",
         "metrics": {"bronze_usage_events": {"rows_dropped_by_watermark": dropped}}},
        {"stage": "silver", "status": "ok", "metrics": {
            "support_tickets": {"quarantined": quarantined},
            "usage_events": {"silver_rows": 100, "quarantined_rows": 0, "o3_breached": False,
                             "quarantined_rows_pct": 0.0, "quarantined_cost_pct": 0.0}}},
        {"stage": "gold", "status": "ok", "metrics": {"org_daily_usage_by_service": {"passed": True, "gold_rows": 10}}},
        {"stage": "serving", "status": "ok",
         "metrics": {"t": {"rows_written": written, "gold_rows": 10}, "target": "cassandra"}},
    ]
    if failed_stage:
        stages = [{**s, "status": "failed", "error": "boom"} if s["stage"] == failed_stage else s for s in stages]
    return {"run_id": run_id, "status": "failed" if failed_stage else status, "stages": stages,
            "finished_utc": (NOW - timedelta(hours=hours_ago)).isoformat()}


def levels(runs):
    return {name: level for level, name, _ in health.checks(runs, NOW)}


def test_healthy_runs_have_no_alert():
    result = levels([run("20261201T090000Z"), run("20261201T100000Z")])
    assert set(result.values()) == {"OK"}


def test_failed_stage_dropped_rows_and_serving_mismatch_are_alerts():
    assert levels([run("a", failed_stage="silver")])["last run"] == "ALERT"
    assert levels([run("a", dropped=3)])["stream watermark"] == "ALERT"
    assert levels([run("a", written=9)])["serving row check"] == "ALERT"


def test_stale_gold_and_growing_quarantine_are_warnings():
    result = levels([run("a", hours_ago=60, quarantined=1), run("b", hours_ago=30, quarantined=4)])
    assert result["freshness"] == "WARN" and result["Quarantine growth"] == "WARN"


def test_exit_code_follows_alerts(tmp_path, monkeypatch):
    (tmp_path / "20261201T100000Z.json").write_text(json.dumps(run("20261201T100000Z", failed_stage="gold")))
    (tmp_path / "20261201T100000Z_anomaly_summary.json").write_text("{}")   # not a run summary
    monkeypatch.setenv("LOG_DIR", str(tmp_path))
    out = tmp_path / "health.md"
    assert health.main(["--out", str(out)]) == 1
    assert "failed at gold" in out.read_text()


def test_astra_host_is_masked():
    text = "cannot reach https://fda5a815-d29b-41e1-9d2a-21f016a85cde-us-east-2.db.astra.datastax.com:29080/metadata"
    assert "datastax" not in health.masked(text)
