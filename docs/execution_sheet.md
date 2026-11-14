# Ficha de ejecución

Cómo corre el pipeline hoy, medido en la corrida de la evidencia, y cómo se traduciría a containers
de YARN a la escala proyectada del apéndice A del diseño.

## Hoy: un proceso local

| Recurso | Valor | Por qué |
| :--- | :--- | :--- |
| Master | `local[4]` | un JVM con driver y executor juntos y 4 slots de tarea |
| Memoria | `spark.driver.memory=2g` | el dataset completo son 13 MB de JSON y 7 MB de Parquet |
| `spark.sql.shuffle.partitions` | 8 | con 200 (el valor por defecto) cada agregación lanza 200 tareas casi vacías. 8 son dos rondas de 4 slots, y AQE junta las que quedan chicas |
| AQE | activado | coalesce de particiones de shuffle después de medir el tamaño real |
| Archivos por partición | 1 (`FILES_PER_PARTITION`) | un archivo de 10 a 150 KB por día o por micro-batch (design §5.1) |
| Java | JDK 21 (`PIPELINE_JAVA_HOME`) | Spark 4.2 corre sobre 17, 21 y 25; el pipeline acepta los tres, y la evidencia se generó con 21 |

## Jobs y lo que midió la primera corrida

Primera corrida de `make rerun-evidence`, sobre un lake vacío. Una etapa del pipeline lanza varios jobs
de Spark: cada `count`, cada escritura y cada lectura de métricas es un job.

| Etapa | Qué hace | Trigger | Segundos | Jobs | Tareas | Shuffle escrito |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: |
| `bronze-batch` | 7 CSV a Bronze, registro de ingestados | por corrida | 10,4 | 64 | 64 | 51 KB |
| `bronze-stream` | 120 JSONL a Bronze y Quarantine, 2 queries | `availableNow`, 10 archivos por micro-batch | 7,4 | 25 | 109 | 4.569 KB |
| `silver` | 7 maestros y eventos conformados, reglas, joins, dedupe | por corrida | 8,6 | 63 | 141 | 6.176 KB |
| `gold` | `org_daily_usage_by_service`, conciliación | por corrida | 2,3 | 20 | 126 | 964 KB |
| `anomalies` | `cost_anomaly_mart` | por corrida | 4,4 | 45 | 235 | 1.287 KB |
| `serving` | upsert de los dos marts en Cassandra y verificación | por corrida | 3,9 | 12 | 148 | 1.056 KB |

La corrida completa tarda unos 37 segundos y la re-ejecución unos 29 (los tiempos varían entre corridas y con la carga de la máquina): el stream no lee nada y la carga batch
saltea los 7 CSV registrados. Silver, Gold y la carga se rehacen enteros en las dos (D8).

El shuffle más grande es el de Silver: el dedupe por `event_id` es una ventana particionada por
`event_id` y mueve todos los eventos. La agregación de Gold mueve menos porque agrega parcial antes
del exchange, que es el combiner del §7 del diseño (nodos 2 a 4 del plan en `docs/entrega-2.md` §3.3).

## A escala: cómo quedaría en YARN

Supuestos del apéndice A: unos 43.200 millones de eventos por día, 500.000 por segundo, 13 TB de JSON
y 1,6 TB de Parquet por día.

| Job | Cola | Executors | Container por executor | Paralelismo |
| :--- | :--- | :--- | :--- | :--- |
| Streaming a Bronze | `streaming`, 30% de capacidad mínima, aplicación de larga duración | 10 executors × 4 cores, sin dynamic allocation | 8 GB + 10% de overhead, redondeado a 9 GB y 4 vcores | unos 150 MB/s de JSON, 9 GB por micro-batch de un minuto |
| Silver diario | `batch`, 70%, elástica hasta 100% de noche | dynamic allocation hasta 100 × 5 cores | 16 GB + overhead, 18 GB y 5 vcores | 12.500 splits de 128 MB, unas 25 rondas de 500 tareas; `shuffle.partitions` cerca de 8.000 para unos 200 MB por partición en el dedupe |
| Gold diario | `batch` | los del job de Silver | igual | 300.000 claves por día después del combiner; 200 particiones de shuffle alcanzan |
| Carga a Cassandra | `batch` | 10 × 2 cores | 4 GB | 300.000 upserts por día, acotado por el cluster de Cassandra y no por Spark |

En modo cluster, cada aplicación pide además un ApplicationMaster que hospeda al driver: unos 4 GB
y 1 vcore. YARN asigna containers en múltiplos de `yarn.scheduler.minimum-allocation-mb`, por eso
cada executor pide su memoria más el overhead redondeado hacia arriba. Con el Capacity Scheduler, la
cola `streaming` tiene una capacidad mínima reservada para que el batch nocturno nunca le saque recursos al
stream, y la cola `batch` puede usar lo que el stream no ocupa entre las 00:00 y las 06:00 UTC, que es
la ventana de O1.

El estado del dedupe es lo que más cambia con la escala. Hoy son 43.200 claves. A 500.000 eventos
por segundo, una hora de watermark son unos 1.800 millones de `event_id`, del orden de 180 GB. Eso
pide el state store de RocksDB (`spark.sql.streaming.stateStore.providerClass`), que guarda el estado
en el disco local de cada executor, y un watermark más corto, de unos 10 minutos, que sigue
cubriendo el horizonte de un reintento. La deduplicación exacta queda en Silver, donde no tiene
costo de estado.
