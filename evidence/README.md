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

Salida de las corridas de la entrega. `make rerun-evidence` vuelve a correr el pipeline dos veces y
refresca `counts_run_1.json` y `counts_run_2.json`.

| Archivo | Contenido |
| :--- | :--- |
| `run_1_<run_id>.log`, `run_2_<run_id>.log` | log de la primera corrida, sobre un lake vacío, y de la re-ejecución |
| `counts_run_1.json`, `counts_run_2.json` | filas por tabla del lake y de Cassandra después de cada corrida |
| `stream_bronze.md`, `run_1_stream_progress.jsonl` | micro-batches del stream y la medición de late data |
| `quarantine_samples.md` | una muestra cruda de cada regla de Quarantine |
| `serving_q1_cqlsh.txt`, `serving_q2_cqlsh.txt`, `serving_q6_cqlsh.txt` | Q1, Q2 y Q6 por cqlsh contra Cassandra local |
| `serving_q*_driver_astra.txt`, `astra_load_1.log` | las mismas consultas y la carga de Gold contra AstraDB |
| `delivery_2_checklist.md` | checklist §9.2 y criterios §6.4, en `evidence/` |

En los archivos copiados de los logs, la ruta local del repo aparece como `<repo>`, y el token, el
host de AstraDB y la ruta del bundle quedan enmascarados.
