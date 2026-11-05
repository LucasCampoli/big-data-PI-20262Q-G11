# Checklist de la entrega 2

Verificado el 2026-11-05 contra la consigna, sobre la evidencia de `make rerun-evidence`. La puerta
de entrada es [`../docs/entrega-2.md`](../docs/entrega-2.md).

## Checklist §9.2

| # | Ítem | Dónde está | Evidencia | Estado |
| :-- | :--- | :--- | :--- | :--- |
| 1 | Correcciones de la primera entrega incorporadas | `docs/entrega-2.md` §1 | la entrega 1 no tuvo correcciones; los cambios que trajo la implementación están en D3, D5, D6 y D13 a D17 | Hecho |
| 2 | Arquitectura actualizada | `docs/architecture_v2.svg`, `docs/entrega-2.md` §2 | el diagrama muestra solo lo que corre | Hecho |
| 3 | Tres maestros batch en Bronze | `src/pipeline/bronze_batch.py` | los 7 CSV en `entrega-2/lake_inventory.md`, billing particionado por `month` | Hecho |
| 4 | Streaming con esquema, watermark, dedupe, late data y checkpoint | `src/pipeline/bronze_stream.py`, D14 | `entrega-2/stream_bronze.md`, `run_1_stream_progress.jsonl`, `stream_state_demo_progress.jsonl` | Hecho |
| 5 | Silver para eventos y al menos un maestro | `src/pipeline/silver.py` | eventos y los 7 maestros en `entrega-2/silver_quality.md` | Hecho |
| 6 | Tres features calculadas | `src/pipeline/silver.py`, `src/pipeline/gold.py` | `daily_cost_usd`, `requests`, `cpu_hours`, `storage_gb_hours`, `genai_tokens`, `carbon_kg` en `entrega-2/data_dictionary.md` | Hecho |
| 7 | Tres reglas de calidad y quarantine | `src/pipeline/quality_rules.py`, D6, D7, D15 | `entrega-2/silver_quality.md`, `entrega-2/quarantine_samples.md` | Hecho |
| 8 | Mart `org_daily_usage_by_service` en Gold | `src/pipeline/gold.py` | `entrega-2/gold_marts.md`, `gold_reconciliation.json`, `gold_explain.txt` | Hecho |
| 9 | Keyspace y tabla Cassandra/AstraDB poblada | `cql/`, `src/pipeline/serving.py`, D11, D16 | 11.050 filas por tabla en `entrega-2/idempotency.md` | Hecho en Cassandra local; AstraDB soportado, sin correr |
| 10 | Dos consultas CQL con resultados | `cql/q1_*.cql`, `cql/q2_*.cql` | `entrega-2/serving_q1_*.txt`, `serving_q2_*.txt` | Hecho |
| 11 | Idempotencia demostrada | `make rerun-evidence` | `entrega-2/idempotency.md`, `counts_run_*.json` | Hecho |
| 12 | README Quickstart, logs y evidencias | `README.md`, `logs/`, `evidence/entrega-2/` | `run_*_<run_id>.log` y `.json` | Hecho, validado en un clon limpio |
| 13 | Backlog final priorizado | `docs/entrega-2.md` §11 | obligatorio, deseable y fuera de alcance | Hecho |

## Criterios de aceptación §6.4

| # | Criterio | Cómo se verifica | Estado |
| :-- | :--- | :--- | :--- |
| 1 | Batch y streaming corren con los datos provistos | `make run` sobre `data/landing/`; 43.200 eventos y 4.112 filas de maestros en Bronze | Hecho |
| 2 | Las reglas de calidad y quarantine son efectivas y tienen ejemplos verificables | `make fixtures` y `make test`; una muestra cruda por regla en `quarantine_samples.md` | Hecho |
| 3 | El mart FinOps existe en Gold y la tabla de Cassandra está poblada | `gold_marts.md`; carga verificada por organización contra Gold | Hecho |
| 4 | Se ejecutan dos consultas mínimas sobre AstraDB con resultados | Q1 y Q2 corren sobre Cassandra local con cqlsh y con el driver; el mismo código corre sobre AstraDB con `SERVING_TARGET=astra` (README) | Hecho sobre Cassandra; AstraDB pendiente de credenciales |
| 5 | La re-ejecución no duplica registros | `idempotency.md`: mismos conteos en 18 tablas del lake, 2 de Cassandra y el registro | Hecho |
| 6 | El diagrama representa lo implementado | `architecture_v2.svg` sin speed layer; AstraDB mencionado en el texto, no en la figura | Hecho |
| 7 | El README permite ejecutar desde un entorno limpio | Quickstart probado en un clon nuevo con el dataset copiado según el README | Hecho |
