# big-data-PI-20262Q-G11

Integrative project for Big Data, group 11. Cloud Provider Analytics: ingest and shape customer data for FinOps, Support, and Product.

This repo is still at the design stage. Implementation comes later.

Design notes so far: [docs/design.md](docs/design.md) (problem, users, goals, the 5Vs, and a first profile of the landing files).

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

## Running the exploration notebook

The Landing files are not committed (see `data/README.md`). Put them under `data/landing/` so the
tree looks like `data/landing/*.csv` plus `data/landing/usage_events_stream/*.jsonl`, then:

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
