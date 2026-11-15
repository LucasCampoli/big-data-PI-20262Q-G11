# Evidencia

Artefactos que respaldan lo que afirman los documentos de cada entrega. Salen de las corridas y no
se editan a mano. Los generados están en inglés, como el código; este README y el checklist de la
entrega 1 los escribimos nosotros y van en español.

## Entrega 1

| Archivo | Contenido |
| :--- | :--- |
| `landing_profile.md` | perfil de Landing: grano y filas por fuente, nulos, versiones de esquema, `value` como número y como texto, costos, late data, particionado y la base de cada regla de calidad |
| `landing_source_profile.csv`, `landing_column_profile.csv` | una fila por fuente y una por columna |
| `landing_exploration_run.log` | log de la última corrida del notebook (`scripts/run_exploration.sh`) |
| `delivery_1_checklist.md` | checklist §9.1 y criterios §5.4 |

Los tres perfiles salen de `notebooks/01_landing_exploration.ipynb`.

## Entrega 2, `entrega-2/`

Salida de `make rerun-evidence` del 2026-11-08: el pipeline completo dos veces seguidas desde un
lake vacío, con los datos reales.

| Archivo | Contenido | Cómo se obtiene |
| :--- | :--- | :--- |
| `counts_run_1.json`, `counts_run_2.json` | filas por tabla del lake y de Cassandra después de cada corrida | `make rerun-evidence` los vuelve a escribir |
| `serving_q1_cqlsh.txt`, `serving_q2_cqlsh.txt`, `serving_q6_cqlsh.txt` | Q1, Q2 y Q6 por cqlsh contra Cassandra local | `make queries` los vuelve a escribir |
| `serving_q*_driver_astra.txt` | las mismas consultas por el driver contra AstraDB | `python -m pipeline.queries` con `SERVING_TARGET=astra` (README, AstraDB) |
| `run_1_<run_id>.log`, `run_2_<run_id>.log`, `astra_load_1.log` | log de la primera corrida, de la re-ejecución y de la carga a AstraDB | copia de `logs/<run_id>.log` |
| `run_1_stream_progress.jsonl` | progreso de cada micro-batch del stream | copia de `logs/<run_id>_stream_progress.jsonl` |
| `gold_explain.txt` | plan físico de la agregación de Gold (`df.explain()`) | copia de `logs/<run_id>_gold_explain.txt` |
| `stream_bronze.md` | resumen de los micro-batches y la medición de late data | generado en esa corrida por un reporte que después se sacó del pipeline; los datos de base están en `run_1_stream_progress.jsonl` |
| `quarantine_samples.md` | una muestra cruda de cada regla de Quarantine | generado en esa corrida por el mismo reporte; `make fixtures` vuelve a producir las filas en `datalake/_fixtures/quarantine/` |

En los archivos copiados de los logs, la ruta local del repo aparece como `<repo>`, y el token, el
host de AstraDB y la ruta del bundle quedan enmascarados.
