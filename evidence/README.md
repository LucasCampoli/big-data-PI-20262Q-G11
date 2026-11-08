# Evidencia

Artefactos generados que respaldan lo que afirman los documentos de cada entrega. No se editan a
mano: se regeneran con el comando que los produce. Los reportes generados están en inglés, como el
código; este README y los checklists los escribimos nosotros y van en español.

## Entrega 1

| Archivo | Contenido |
| :--- | :--- |
| `landing_profile.md` | perfil de Landing: grano y filas por fuente, nulos, versiones de esquema, `value` como número y como texto, costos, late data, particionado y la base de cada regla de calidad |
| `landing_source_profile.csv`, `landing_column_profile.csv` | una fila por fuente y una por columna |
| `landing_exploration_run.log` | log de la última corrida del notebook (`scripts/run_exploration.sh`) |
| `delivery_1_checklist.md` | checklist §9.1 y criterios §5.4 |

Los tres perfiles salen de `notebooks/01_landing_exploration.ipynb`.

## Entrega 2, `entrega-2/`

Todo sale de `make rerun-evidence`, que resetea el lake y el keyspace, corre el pipeline completo
dos veces seguidas, corre el experimento de estado, los fixtures y las consultas, y regenera los
reportes con `python -m pipeline.evidence report`.

| Archivo | Contenido |
| :--- | :--- |
| `run_1_<run_id>.log`, `run_1_<run_id>.json` | log y resumen de la primera corrida, sobre un lake vacío |
| `run_2_<run_id>.log`, `run_2_<run_id>.json` | lo mismo para la re-ejecución |
| `counts_run_1.json`, `counts_run_2.json`, `idempotency.md` | filas por tabla del lake y de Cassandra después de cada corrida, y la comparación |
| `lake_inventory.md` | rutas, filas, archivos, particiones y bytes de cada tabla |
| `data_dictionary.md` | esquema de cada tabla leído desde el Parquet, con notas de las columnas técnicas |
| `stream_bronze.md`, `run_*_stream_progress.jsonl`, `stream_state_demo_progress.jsonl` | progreso de cada micro-batch, el experimento con watermark de 2 s y la medición de late data |
| `silver_quality.md` | las reglas de la consigna medidas en Silver, O3 y los flags de los maestros |
| `quarantine_samples.md`, `fixtures_run.log` | filas en Quarantine por tabla y por regla, con una muestra cruda de cada regla |
| `gold_marts.md`, `gold_reconciliation.json`, `gold_explain.txt`, `cost_anomaly_summary.json` | conciliación de Gold, plan físico de la agregación y resumen de anomalías |
| `serving_q1_*.txt`, `serving_q2_*.txt`, `serving_q6_*.txt` | cada consulta por cqlsh y por el driver, con el CQL ejecutado; los `_driver_astra` corren contra AstraDB |
| `serving_astra.md`, `astra_load_1.*`, `astra_load_2.*`, `counts_astra_load_*.json` | las dos cargas de Gold en AstraDB, sus logs y los conteos después de cada una |
| `execution_profile.md` | jobs, stages, tareas y bytes de shuffle por etapa, desde el event log de Spark |
| `delivery_2_checklist.md` | checklist §9.2 y criterios §6.4, en `evidence/` |

En los archivos copiados de los logs, la ruta local del repo aparece como `<repo>`, y el token, el
host de AstraDB y la ruta del bundle quedan enmascarados.
