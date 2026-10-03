# big-data-PI-20262Q-G11

Integrative project for Big Data, group 11. Cloud Provider Analytics: ingest and shape customer data for FinOps, Support, and Product.

This repo is still at the design stage. Implementation comes later.

Design document: [docs/design.md](docs/design.md). Problem and objectives, the 5Vs, the source
profile, the architectural pattern, the Data Lake, architecture v1, the MapReduce reference flow and
the plan.

Data evidence: [evidence/landing_profile.md](evidence/landing_profile.md), produced by
[notebooks/01_landing_exploration.ipynb](notebooks/01_landing_exploration.ipynb).

Decisions, with alternatives and trade-offs: [DECISIONS.md](DECISIONS.md).

## Layout

```text
README.md
DECISIONS.md
requirements.txt
docs/          design notes and the course brief
data/          sample data; raw files go in data/landing/
src/           processing code (delivery 2)
notebooks/     PySpark exploration of Landing
tests/         tests and quarantine fixtures (delivery 2)
config/        paths.env.example — copy and adjust locally, no secrets
infra/         runtime notes (later)
evidence/      generated profiles and logs for deliveries
```

## Getting the dataset

The Landing files are not committed, so each person gets their own copy. Download the Cloud Provider
Analytics challenge dataset handed out by the course and unzip it, then copy the contents of its
`datalake/landing/` directory into `data/landing/` so the tree looks like this:

```text
data/landing/customers_orgs.csv
data/landing/users.csv
data/landing/resources.csv
data/landing/support_tickets.csv
data/landing/marketing_touches.csv
data/landing/nps_surveys.csv
data/landing/billing_monthly.csv
data/landing/usage_events_stream/events_part_0000.jsonl   (120 files)
```

If the archive came from a Windows download, delete the `*:Zone.Identifier` files it may carry.

`data/landing/` is git-ignored and Landing is never edited, so a fresh copy is always safe. Point
`DATA_LANDING` at a different directory if you keep the files elsewhere.

## Running the exploration notebook

With the dataset in place:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/jupyter lab notebooks/01_landing_exploration.ipynb
```

PySpark needs a JDK 17 or 21 on the path. Set `JAVA_HOME` if your default is newer, and
`DATA_LANDING` if the Landing files live somewhere other than `data/landing/`.

Running all cells rewrites the three files in `evidence/`, so the committed numbers are reproducible
from the Landing data.

Copy `config/paths.env.example` if you need a local path override. Do not commit `.env` or credentials.

## Conventions

Everything in the repo is in English: code, documentation and commit messages.

Branches. `main` holds what gets delivered, and it is what the deadline is evaluated on. Work on
short-lived branches named `<area>/<topic>`, for example `silver/quality-rules` or
`serving/keyspace`, and merge into `main` when the piece is complete. Areas follow the roles in
[docs/design.md](docs/design.md) §8.4: `ingestion`, `silver`, `marts`, `serving`, `docs`.

Commits. A short subject in the imperative, under about 60 characters, saying what the commit does:
"Add the streaming job", not "added streaming" or "changes". Use the body only when the reason is
not obvious from the diff, and keep it brief. List co-authors with `Co-authored-by:` trailers when
the work was done together. One logical change per commit.

Naming.

| Thing | Convention | Example |
| :--- | :--- | :--- |
| Python modules and functions | `snake_case` | `quality_rules.py` |
| Notebooks | `NN_topic.ipynb`, numbered in run order | `01_landing_exploration.ipynb` |
| Lake paths | `datalake/<zone>/<entity>/<partition>=<value>/` | `datalake/silver/usage_events/event_date=2025-08-01/` |
| Columns | `snake_case` | `cost_usd_increment` |
| Technical columns | fixed set, same names in every zone | `ingest_ts`, `ingest_date`, `source_file`, `processed_ts`, `run_id` |
| Repair and quality flags | boolean, named for what happened | `unit_imputed`, `fx_overridden`, `cost_anomaly_flag`, `dq_status` |
| Gold marts | `<grain>_<subject>_by_<dimension>` | `org_daily_usage_by_service` |
| Evidence files | `<source>_<artifact>.<ext>`, regenerated not hand-edited | `landing_profile.md` |
