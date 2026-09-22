# big-data-PI-20262Q-G11

Integrative project for Big Data, group 11. Cloud Provider Analytics: ingest and shape customer data for FinOps, Support, and Product.

This repo is still at the design stage. Implementation comes later.

Design notes so far: [docs/design.md](docs/design.md) (problem, users, goals, and the 5Vs).

## Layout

```text
README.md
DECISIONS.md
requirements.txt
docs/          design notes and the course brief
data/          sample data; raw files go in data/landing/
src/           ingestion and processing code (later)
notebooks/     exploration (later)
tests/
config/        paths.env.example — copy and adjust locally, no secrets
infra/         runtime notes (later)
evidence/      logs and screenshots for deliveries
```

Copy `config/paths.env.example` if you need a local path override. Do not commit `.env` or credentials.
