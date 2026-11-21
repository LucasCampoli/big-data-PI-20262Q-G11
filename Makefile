# Entry points for the pipeline. Every target runs from the repo root.
PY ?= .venv/bin/python
export PYTHONPATH := src
RUN := $(PY) -m pipeline.run

.PHONY: setup check run bronze stream stream-state-demo silver gold serve queries cqlsh cassandra-up cassandra-init cassandra-down fixtures test reset reset-stream reset-serving evidence rerun-evidence health dictionary

setup:            ## create .venv, install pinned dependencies, create or complete config/pipeline.env
	python3 -m venv .venv
	.venv/bin/pip install -q -r requirements.txt
	python3 scripts/init_env.py

check:            ## Java, Spark and Landing sanity check
	$(RUN) check

run:              ## the whole pipeline, Landing to serving
	$(RUN) all

test:             ## unit tests (Spark local, no Cassandra needed)
	$(PY) -m pytest

reset:            ## delete the lake, the checkpoints and the ingest registry (Landing is never touched)
	rm -rf datalake

bronze:           ## batch load of the CSV masters and billing
	$(RUN) bronze-batch

stream:           ## streaming load of the event parts (trigger from STREAM_TRIGGER)
	$(RUN) bronze-stream

stream-state-demo: ## same stream with a 2 s watermark in a scratch lake: state is evicted, nothing dropped
	rm -rf datalake/_experiments/watermark_2s
	STREAM_WATERMARK="2 seconds" STREAM_MAX_FILES_PER_TRIGGER=4 \
	DATALAKE_ROOT=datalake/_experiments/watermark_2s $(RUN) bronze-stream

reset-stream:     ## new checkpoints: drop both streaming queries' state and outputs, reprocess from Landing
	rm -rf datalake/_checkpoints/bronze_usage_events datalake/_checkpoints/quarantine_usage_events_parse \
	       datalake/bronze/usage_events datalake/quarantine/usage_events_parse

silver:           ## rebuild Silver and the rule Quarantine from Bronze
	$(RUN) silver

gold:             ## daily FinOps mart (reconciled against Silver), the anomaly mart and the Q2-Q5 marts
	$(RUN) gold anomalies marts

fixtures:         ## the pipeline over the synthetic fixtures (tests/fixtures/landing) in a separate lake
	rm -rf datalake/_fixtures logs/fixtures
	DATA_LANDING=tests/fixtures/landing DATALAKE_ROOT=datalake/_fixtures LOG_DIR=logs/fixtures \
	SERVING_TARGET=none $(RUN) bronze-batch bronze-stream silver gold anomalies marts

COMPOSE := docker compose -f infra/docker-compose.yml
# cqlsh logs in as a role; the credentials come from the same place the pipeline reads them.
CONF = $$($(PY) -m pipeline.config get $(1))
CQLSH_AS = $(COMPOSE) exec -T cassandra cqlsh --request-timeout=120 -u "$(call CONF,$(1)_USERNAME)" -p "$(call CONF,$(1)_PASSWORD)"
# One evidence directory for make and for the pipeline: `EVIDENCE=dir make ...` moves both.
EVIDENCE ?= evidence/final
export EVIDENCE_DIR = $(EVIDENCE)

cassandra-up:     ## start the local Cassandra and wait until it answers CQL
	$(COMPOSE) up -d --wait

cassandra-init:   ## as superuser: keyspace, tables, and the loader and reader roles (D19); idempotent
	$(PY) -m pipeline.cassandra_admin

cassandra-down:   ## stop it (the data volume is kept; add -v by hand to drop it)
	$(COMPOSE) down

serve:            ## create keyspace and tables if missing, upsert the Gold marts, check the row count
	$(RUN) serving

queries:          ## Q1 to Q6 through the driver and, on local Cassandra, through cqlsh; saved to the evidence dir
	$(PY) -m pipeline.queries
	@mkdir -p $(EVIDENCE)
	if [ "$${SERVING_TARGET:-cassandra}" = cassandra ]; then \
	  for f in cql/q*.cql; do q=$$(basename $$f .cql); \
	    { echo "$$ cqlsh -f $$f"; $(call CQLSH_AS,CASSANDRA_READER) -f /$$f; } > $(EVIDENCE)/serving_$${q%%_*}_cqlsh.txt; \
	    cat $(EVIDENCE)/serving_$${q%%_*}_cqlsh.txt; done; fi

cqlsh:            ## interactive cqlsh in the container, as the read-only role
	$(COMPOSE) exec cassandra cqlsh -u "$(call CONF,CASSANDRA_READER_USERNAME)" -p "$(call CONF,CASSANDRA_READER_PASSWORD)"

reset-serving:    ## drop the local Cassandra keyspace; make cassandra-init creates it again, empty
	$(PY) -m pipeline.cassandra_admin --drop

health:           ## OK, WARN or ALERT for the last runs (exit 1 on ALERT), from the summaries in logs/
	$(PY) -m pipeline.health

dictionary:       ## data dictionary of every lake table, read from the Parquet files, in docs/data_dictionary.md
	$(PY) -m pipeline.dictionary

evidence:         ## row counts of the lake and of Cassandra, in $(EVIDENCE)/counts_latest.json
	$(PY) -m pipeline.evidence latest

rerun-evidence:   ## from scratch: the pipeline twice with counts and logs, fixtures, Q1-Q6, health and dictionary
	$(MAKE) reset reset-serving cassandra-init
	$(MAKE) run
	$(PY) -m pipeline.evidence run_1
	$(MAKE) run
	$(PY) -m pipeline.evidence run_2
	$(MAKE) fixtures
	DATALAKE_ROOT=datalake/_fixtures LOG_DIR=logs/fixtures SERVING_TARGET=none $(PY) -m pipeline.evidence fixtures
	$(MAKE) queries
	$(PY) -m pipeline.health --out $(EVIDENCE)/health_report.md
	$(MAKE) dictionary
