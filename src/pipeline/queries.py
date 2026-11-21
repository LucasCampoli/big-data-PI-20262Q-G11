"""Run the serving queries against the configured target and keep their output.

python -m pipeline.queries [--out DIR]

The statements are read from cql/q*.cql, the same files cqlsh runs, so the evidence and the scripts
cannot drift apart. Every query reads one slice of one partition and is printed as it comes back;
the only client step left is Q6 keeping the flagged days of the scored ones.
"""
import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from .config import Config
from .serving import CQL_DIR, connect, connection_settings, cql_statements

log = logging.getLogger("pipeline.queries")

# (output name, CQL file, title, columns to print)
QUERIES = [
    ("q1", "q1_daily_cost_by_service.cql", "Q1. Daily cost and requests per service, one organization, date range",
     ["usage_date", "service", "daily_cost_usd", "requests", "events"]),
    ("q2", "q2_top_services_14d.cql", "Q2. Top 3 services by cost over the last 14 days, one organization",
     ["rank", "service", "cost_14d_usd", "days_with_usage", "window_start"]),
    ("q3", "q3_tickets_30d.cql", "Q3. Critical tickets and SLA breach rate per day, last 30 days, one organization",
     ["ticket_date", "tickets", "critical_tickets", "sla_breached_tickets", "sla_breach_rate", "csat_avg"]),
    ("q4", "q4_revenue_by_month.cql", "Q4. Monthly revenue in USD after credits and taxes, one organization",
     ["month", "invoices", "subtotal_usd", "credits_usd", "taxes_usd", "revenue_usd", "currency",
      "exchange_rate_applied"]),
    ("q5", "q5_genai_tokens_daily.cql", "Q5. GenAI tokens and estimated cost per day, one organization, August",
     ["usage_date", "genai_tokens", "genai_cost_usd", "usd_per_1k_tokens", "events", "events_with_tokens"]),
]


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
        settings = connection_settings(cfg, "reader")
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
    outputs = {}
    try:
        for name, cql_file, title, columns in QUERIES:
            stmt, rows = run_file(session, cql_file, settings["keyspace"])
            outputs[name] = report(title, settings, stmt, table(rows, columns) + f"\n\n({len(rows)} rows)")

        stmt, rows = run_file(session, "q6_cost_anomalies.cql", settings["keyspace"])
        flagged = [r for r in rows if r["is_anomaly"]]
        outputs["q6"] = report("Q6. Days flagged as cost anomalies, one organization, August", settings, stmt,
                               f"{len(rows)} scored rows read, {len(flagged)} with is_anomaly = true:\n\n"
                               + table(flagged, ["usage_date", "service", "daily_cost_usd", "median_cost",
                                                 "anomaly_score", "anomaly_direction"]))
    except Exception as exc:
        log.error("query failed: %s", exc)
        return 1
    finally:
        cluster.shutdown()

    for name, text in outputs.items():
        path = out / f"serving_{name}_driver{suffix}.txt"
        path.write_text(text)
        print(text)
        log.info("wrote %s", path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
