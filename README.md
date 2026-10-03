# big-data-PI-20262Q-G11

Integrative project for Big Data, group 11. Cloud Provider Analytics: ingest and shape customer data for FinOps, Support, and Product.

This repo is still at the design stage. Implementation comes later.

Design notes so far: [docs/design.md](docs/design.md) (problem, users, goals, the 5Vs, and a first profile of the landing files).

Data evidence: [evidence/landing_profile.md](evidence/landing_profile.md), produced by
[notebooks/01_landing_exploration.ipynb](notebooks/01_landing_exploration.ipynb).

## Layout

```text
README.md
DECISIONS.md
requirements.txt
docs/          design notes and the course brief
data/          sample data; raw files go in data/landing/
src/           ingestion and processing code (later)
notebooks/     PySpark exploration of Landing
tests/
config/        paths.env.example — copy and adjust locally, no secrets
infra/         runtime notes (later)
evidence/      generated profiles and logs for deliveries
```

## Running the exploration notebook

The Landing files are not committed (see `data/README.md`). Put them under `data/landing/` so the
tree looks like `data/landing/*.csv` plus `data/landing/usage_events_stream/*.jsonl`, then:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/jupyter lab notebooks/01_landing_exploration.ipynb
```

PySpark needs a JDK 17 or 21 on the path; set `JAVA_HOME` if your default is a newer one. Set
`DATA_LANDING` if your copy of the Landing files lives somewhere other than `data/landing/`.

Running all cells rewrites the three files in `evidence/`, so the committed numbers can always be
reproduced from the Landing data.

Copy `config/paths.env.example` if you need a local path override. Do not commit `.env` or credentials.
