# Entry points for the pipeline. Every target runs from the repo root.
PY ?= .venv/bin/python
export PYTHONPATH := src
RUN := $(PY) -m pipeline.run

.PHONY: setup check run bronze stream stream-state-demo silver gold serve test reset reset-stream

setup:            ## create .venv, install pinned dependencies, create config/pipeline.env
	python3 -m venv .venv
	.venv/bin/pip install -q -r requirements.txt
	test -f config/pipeline.env || cp config/pipeline.env.example config/pipeline.env

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
