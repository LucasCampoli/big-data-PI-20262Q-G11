# Entry points for the pipeline. Every target runs from the repo root.
PY ?= .venv/bin/python
export PYTHONPATH := src
RUN := $(PY) -m pipeline.run

.PHONY: setup check run bronze stream silver gold serve test reset

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
