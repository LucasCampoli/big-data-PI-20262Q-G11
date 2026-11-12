# Entry points for the pipeline. Every target runs from the repo root.
PY ?= .venv/bin/python
export PYTHONPATH := src
RUN := $(PY) -m pipeline.run

.PHONY: setup check run bronze stream stream-state-demo silver gold serve queries cqlsh cassandra-up cassandra-down fixtures test reset reset-stream reset-serving evidence rerun-evidence

setup:            ## create .venv, install pinned dependencies, create config/pipeline.env
	python3 -m venv .venv
	.venv/bin/pip install -q -r requirements.txt
	test -f config/pipeline.env || { cp config/pipeline.env.example config/pipeline.env; \
	  sed -i "s/^PII_HASH_SALT=$$/PII_HASH_SALT=$$(python3 -c 'import secrets; print(secrets.token_hex(16))')/" config/pipeline.env; }

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

gold:             ## daily FinOps mart (reconciled against Silver) and the anomaly mart
	$(RUN) gold anomalies

fixtures:         ## the pipeline over the synthetic fixtures (tests/fixtures/landing) in a separate lake
	rm -rf datalake/_fixtures logs/fixtures
	DATA_LANDING=tests/fixtures/landing DATALAKE_ROOT=datalake/_fixtures LOG_DIR=logs/fixtures \
	SERVING_TARGET=none $(RUN) bronze-batch bronze-stream silver gold anomalies

COMPOSE := docker compose -f infra/docker-compose.yml
CQLSH := $(COMPOSE) exec -T cassandra cqlsh --request-timeout=120
EVIDENCE := evidence/entrega-2

cassandra-up:     ## start the local Cassandra and wait until it answers CQL
	$(COMPOSE) up -d --wait

cassandra-down:   ## stop it (the data volume is kept; add -v by hand to drop it)
	$(COMPOSE) down

serve:            ## create keyspace and tables if missing, upsert the Gold marts, check the row count
	$(RUN) serving

queries:          ## Q1, Q2 and Q6 through the driver and through cqlsh, saved to evidence/entrega-2
	$(PY) -m pipeline.queries
	@mkdir -p $(EVIDENCE)
	for q in q1_daily_cost_by_service q2_top_services_14d q6_cost_anomalies; do \
	  { echo "$$ cqlsh -f cql/$$q.cql"; $(CQLSH) -f /cql/$$q.cql; } > $(EVIDENCE)/serving_$${q%%_*}_cqlsh.txt; \
	  cat $(EVIDENCE)/serving_$${q%%_*}_cqlsh.txt; done

cqlsh:            ## interactive cqlsh in the container
	$(COMPOSE) exec cassandra cqlsh

reset-serving:    ## drop the Cassandra keyspace; the next make serve recreates it empty
	$(CQLSH) -e "DROP KEYSPACE IF EXISTS cloud_analytics;"

evidence:         ## row counts of the lake and of Cassandra, in evidence/entrega-2/counts_latest.json
	$(PY) -m pipeline.evidence latest

rerun-evidence:   ## from scratch: the full pipeline twice, with a row count after each run
	$(MAKE) reset reset-serving
	$(MAKE) run
	$(PY) -m pipeline.evidence run_1
	$(MAKE) run
	$(PY) -m pipeline.evidence run_2
	$(MAKE) stream-state-demo fixtures queries
