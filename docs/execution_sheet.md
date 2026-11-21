# Ficha de ejecución

Cómo corre el pipeline hoy, medido en la corrida de la evidencia.

## Hoy: un proceso local

| Recurso | Valor | Por qué |
| :--- | :--- | :--- |
| Master | `local[4]` | un JVM con driver y executor juntos y 4 slots de tarea |
| Memoria | `spark.driver.memory=2g` | el dataset completo son 13 MB de JSON y 7 MB de Parquet |
| `spark.sql.shuffle.partitions` | 8 | con 200 (el valor por defecto) cada agregación lanza 200 tareas casi vacías. 8 son dos rondas de 4 slots, y AQE junta las que quedan chicas |
| AQE | activado | coalesce de particiones de shuffle después de medir el tamaño real |
| Archivos por partición | 1 (`FILES_PER_PARTITION`) | un archivo de 10 a 150 KB por día o por micro-batch (design §5.1) |
| Java | JDK 21 (`PIPELINE_JAVA_HOME`) | Spark 4.2 corre sobre 17, 21 y 25; el pipeline acepta los tres, y la evidencia se generó con 21 |

## Tiempos de la corrida de la evidencia

Las dos corridas de `make rerun-evidence` (`evidence/final/run_1_*.log` y `run_2_*.log`): la primera
sobre un lake vacío, la segunda sin nada nuevo que ingerir. Segundos por etapa, del resumen de cada
corrida.

| Etapa | Qué hace | Trigger | Primera | Re-ejecución |
| :--- | :--- | :--- | ---: | ---: |
| `bronze-batch` | 7 CSV a Bronze, registro de ingestados | por corrida | 12,4 | 2,4 |
| `bronze-stream` | 120 JSONL a Bronze y Quarantine, 2 queries | `availableNow`, 10 archivos por micro-batch | 8,4 | 1,1 |
| `silver` | 7 maestros y eventos conformados, reglas, joins, dedupe | por corrida | 10,0 | 13,1 |
| `gold` | `org_daily_usage_by_service`, conciliación | por corrida | 2,3 | 2,7 |
| `anomalies` | `cost_anomaly_mart` | por corrida | 3,2 | 3,1 |
| `marts` | los 5 marts de Q2 a Q5, con sus controles | por corrida | 5,7 | 7,0 |
| `serving` | upsert de las 6 tablas en Cassandra y verificación | por corrida | 5,4 | 6,4 |
| total | | | 47,3 | 35,9 |

En la re-ejecución el stream no lee nada y la carga batch saltea los 7 CSV registrados; Silver, Gold,
los marts y la carga se rehacen enteros en las dos (D8). Los tiempos varían entre corridas y con la
carga de la máquina. La cantidad de jobs, tareas y shuffle por etapa la medimos con el event log en
la entrega 2 (`docs/entrega-2.pdf`, ficha de ejecución): Silver es la etapa que más mueve, unos 6 MB
de shuffle.

El shuffle más grande es el de Silver: el dedupe por `event_id` es una ventana particionada por
`event_id` y mueve todos los eventos. La agregación de Gold mueve menos porque agrega parcial antes
del exchange, que es el combiner del §7 del diseño (nodos 2 a 4 de `evidence/final/run_1_gold_explain.txt`).
