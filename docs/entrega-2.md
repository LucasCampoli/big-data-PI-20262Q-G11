# Cloud Provider Analytics, entrega 2

Proyecto integrador de Big Data, ITBA, segundo cuatrimestre 2026.
Segunda entrega: implementación técnica y MVP end-to-end mínimo. Versión 2, 2026-11-15.

Grupo 11: Ivan Odzomek, Lucas Campoli, Matias Sapino, Diego Rabinovich, Diego Badin.

El pipeline corre de punta a punta sobre los datos provistos: Landing, Bronze, Silver, Gold y
Cassandra, con batch y streaming, reglas de calidad con Quarantine, un mart FinOps, un componente de
anomalías y evidencia de idempotencia. Este documento resume lo implementado y remite a la
evidencia. Los pasos exactos para correrlo están en el `README.md`, las decisiones en
`DECISIONS.md` y la evidencia de corrida en `evidence/entrega-2/`.

## 1. Qué cambió desde la entrega 1

La entrega 1 no tuvo correcciones de la cátedra. Lo que cambió en el diseño viene de implementarlo:

| Cambio | Por qué | Decisión |
| :--- | :--- | :--- |
| La vista intradía provisoria (speed layer) no está, y sale del diagrama | la entrega 2 pide el mart diario; queda en el backlog | D3 |
| El streaming corre con `availableNow` y `processingTime="1 minute"` queda como modo continuo | el pipeline se corre entero, se re-ejecuta y se muestra en vivo | D14 |
| Gold particiona por `usage_date` | es la fecha del evento con el nombre del grano del mart | D5 |
| Los maestros de Bronze particionan por `ingest_date` | cada snapshot queda en el día en que se cargó y la retención borra directorios enteros | D5 |
| La carga a Cassandra usa el driver de Python desde `foreachPartition` | el conector oficial no tiene versión para Spark 4 | D16 |
| Quarantine son cuatro tablas, una por escritor, con `rule_name` | el sink del stream es dueño de su carpeta y las reglas se reconstruyen | D15 |
| Registro de ingestados con sha256, dedupe por clave en Silver | idempotencia de la carga batch | D13 |
| `users.email` se reemplaza por un hash con sal en Silver | es el único dato personal | D17 |
| Spark local en WSL, Cassandra en Docker | el supuesto de §8.1 del diseño preveía este cambio | D16 |

El §2.2 de la consigna pide un entorno equivalente a Colab. Corremos el mismo PySpark 4.2.0 fijado
en `requirements.txt`, en modo `local[4]` sobre WSL2 con JDK 21, y lo validamos con `make check`,
las 39 pruebas de `make test` y dos corridas completas desde un clon limpio, con los mismos conteos.
Cassandra corre en Docker y AstraDB se probó con el mismo código. No lo corrimos en Google Colab
(D16).

## 2. Arquitectura v2

![Cloud Provider Analytics, arquitectura v2, 2026-11-08](architecture_v2.svg)

La figura muestra solo lo que corre. El diseño es el híbrido estilo Lambda de D3, y en esta entrega
están implementados el batch layer y la ingesta streaming a Bronze. El streaming hace append a
Bronze y todo lo demás es batch, porque un watermark sobre el tiempo del evento descartaría casi
todos los eventos (§3.1). El speed layer, la vista intradía provisoria, está diferido y no aparece en
la figura, así que la latencia de Gold y de Cassandra es D+1. El serving tiene dos destinos probados con el
mismo cargador (D16): Cassandra 5.0 en Docker, que es el principal, y AstraDB, que se elige por
configuración.

| Etapa | Comando | Entrada | Salida |
| :--- | :--- | :--- | :--- |
| Carga batch | `make bronze` | 7 CSV de Landing | `bronze/<maestro>`, `quarantine/masters_parse` |
| Streaming | `make stream` | `usage_events_stream/*.jsonl` | `bronze/usage_events`, `quarantine/usage_events_parse` |
| Silver | `make silver` | Bronze | `silver/*`, `quarantine/*_rules` |
| Gold | `make gold` | Silver | `gold/org_daily_usage_by_service`, `gold/cost_anomaly_mart` |
| Serving | `make serve` y `make queries` | Gold | tabla en Cassandra y en AstraDB, salidas de las consultas |

`make run` corre todas las etapas en orden. Cada corrida tiene un `run_id`, escribe
`logs/<run_id>.log` y un resumen JSON con la configuración, el commit y las métricas de cada etapa, y
termina con código 1 si alguna etapa falla.

## 3. Procesamiento con Spark

### 3.1 Bronze

La carga batch lee cada CSV con su `StructType`, compara el encabezado con el esquema
(`enforceSchema=false`) y manda a Quarantine, con la línea cruda, lo que no parsea. Agrega
`ingest_ts`, `ingest_date` y `source_file` (desde `_metadata.file_name`). Billing particiona por
`month` y los otros seis maestros por `ingest_date`, una partición por snapshot cargado (D5).

El streaming son dos queries sobre el mismo directorio, cada una con su `queryName` y su checkpoint,
que leen solo los `*.jsonl` (`pathGlobFilter`), así un `:Zone.Identifier` copiado desde Windows no
corta el stream (D14). La de Bronze separa primero las fallas de parseo, después hace
`withWatermark("ingest_ts", "1 hour")` y `dropDuplicatesWithinWatermark` sobre la clave de dedupe, y
escribe un archivo por micro-batch. Sobre los datos reales:

| Medida | Valor |
| :--- | ---: |
| micro-batches con datos | 12, de 10 archivos y 3.600 eventos cada uno |
| eventos en Bronze | 43.200 |
| filas descartadas por el watermark | 0 |
| filas en el estado del dedupe al final | 43.200 (todas llegaron dentro de la hora) |
| re-ejecución con el mismo checkpoint | 0 filas leídas |

Late data: los archivos llegan desordenados en el tiempo del evento, así que un watermark sobre
`timestamp` descartaría entre el 81% y el 97% de los eventos (`stream_bronze.md`). Por eso el
watermark va sobre `ingest_ts`, que se sella al leer: ningún evento queda afuera y el watermark solo
acota cuánto tiempo el dedupe recuerda un `event_id`. Los eventos tardíos se ubican después, en
Silver, en la partición de su `event_date`, que se reconstruye en cada corrida (D8).

### 3.2 Silver

Silver se reconstruye entero desde Bronze en cada corrida (D8). Los maestros se quedan con la última
fila por clave natural y reciben las reparaciones de D7. Los eventos pasan por:

1. Conformación de `service`, `region`, `metric` y `unit` (minúsculas y sin espacios).
2. Dos joins broadcast, contra `customers_orgs` y `resources`, que agregan industria, plan, estado
   del recurso y dueño del recurso. Un join sin match o un recurso de otra organización son reglas
   de rechazo.
3. Las 8 reglas de rechazo, la imputación de `unit` y el `try_cast` de `value` (D2, D6).
4. Dedupe exacto por `event_id` con una ventana que conserva la primera fila por `ingest_ts`.
5. Features: `requests`, `cpu_hours` y `storage_gb_hours` como una columna por métrica, más
   `cost_usd_increment`, `genai_tokens` y `carbon_kg`.

Silver queda particionado por `event_date`, con 60 particiones y un archivo de unos 46 KB por día.

### 3.3 Gold y el plan de ejecución

`org_daily_usage_by_service` es un `groupBy(org_id, usage_date, service).agg(...)` con
`daily_cost_usd`, `requests`, `cpu_hours`, `storage_gb_hours`, `genai_tokens`, `carbon_kg`, `events`,
`negative_cost_events` y `unit_imputed_events`. `F.sum` devuelve null cuando todas las entradas son
nulas, que es el par suma y conteo del §7 del diseño. Antes de escribir, el mart se concilia contra
Silver: si alguna organización y día difiere en más de 0,01 USD, si hay claves nulas o si la
cantidad de filas no es la del grano, la corrida falla y no publica.

| Control | Resultado |
| :--- | ---: |
| filas en Gold | 11.050 |
| claves del grano en Silver | 11.050 |
| eventos sumados en Gold | 43.200 |
| organización-días comparados | 4.580 |
| diferencia máxima | 0,00 USD |

El plan físico de esa agregación (`df.explain()`) está en `gold_explain.txt`.

### 3.4 Particiones y archivos

| Tabla | Partición | Particiones | Archivos | Tamaño medio |
| :--- | :--- | ---: | ---: | ---: |
| `bronze/usage_events` | `ingest_date` | 1 | 12 | 148 KB |
| `silver/usage_events` | `event_date` | 60 | 60 | 46 KB |
| `gold/org_daily_usage_by_service` | `usage_date` | 60 | 60 | 10 KB |
| `gold/cost_anomaly_mart` | `usage_date` | 60 | 60 | 10 KB |

Las filas de cada tabla de esa corrida están en `counts_run_1.json`.

## 4. Calidad y Quarantine

| Regla | Dónde | Acción | Datos reales | Fixtures |
| :--- | :--- | :--- | ---: | ---: |
| la línea no parsea (JSON o CSV) | Bronze | Quarantine | 0 | 3 |
| `event_id` nulo | Silver | Quarantine | 0 | 1 |
| `event_id` duplicado | stream y Silver | dedupe | 0 | 1 |
| `timestamp` nulo, `metric` desconocido, `unit` contra `metric`, `value` que no castea | Silver | Quarantine | 0 | 4 |
| `org_id` o `resource_id` sin match, recurso de otra organización | Silver | Quarantine | 0 | 3 |
| `cost_usd_increment` < -0,01 | Silver | `cost_anomaly_flag` | 211 | 1 |
| `unit` nulo con `metric` conocido | Silver | imputar, `unit_imputed` | 2.075 | 1 |
| ticket resuelto antes de creado | Silver | Quarantine | 0 | 1 |
| valores fuera de rango en los maestros (CSAT, NPS, tasa USD, créditos y otros) | Silver | anular, reparar o marcar | cantidades por regla en D7 | 1 cada una |

Las tres reglas de la consigna se cumplen en Silver: 0 `event_id` nulos y 0 duplicados, 211 costos
negativos marcados, y los 42.323 eventos con `value` tienen `unit`, o sea el 100% después de la
imputación. Quarantine queda en 0% de los eventos y 0% del costo, contra el 1% de O3. Con los datos
reales Quarantine queda vacía, así que las muestras salen de los fixtures de `tests/fixtures/landing/`,
que ejercitan cada regla con un lake aparte (D10). En la demo se muestran con `make fixtures`. Tres filas de Quarantine, recortadas:

| Tabla | `rule_name` | `source_file` | `raw_record` |
| :--- | :--- | :--- | :--- |
| `quarantine/usage_events_parse` | `parse_type_mismatch` | `events_fixture_0000.jsonl` | `{"event_id": "evt_fixture_ts", "timestamp": "not-a-timestamp", ...}`, la línea cruda |
| `quarantine/usage_events_rules` | `value_not_castable` | `events_fixture_0000.jsonl` | `{"event_id":"evt_fixture_value", ..., "metric":"cpu_hours", "value":"n/a", "unit":"hours", ...}`, la fila de Bronze |
| `quarantine/masters_rules` | `resolved_before_created` | `support_tickets.csv` | `{"ticket_id":"tkt_fixture02", ..., "created_at":"2025-07-05", "resolved_at":"2025-07-02", ...}` |

Una muestra completa de cada regla, con su `raw_record`, está en `quarantine_samples.md`.

## 5. Serving en Cassandra y AstraDB

Cassandra 5.0 corre en Docker (`infra/docker-compose.yml`). El keyspace y las tablas están en
`cql/`, y las consultas en `cql/q*.cql`. AstraDB es el segundo destino, con el mismo esquema y el
mismo cargador.

| Consulta | Tabla | Partition key | Clustering | Lectura |
| :--- | :--- | :--- | :--- | :--- |
| Q1, costo y requests diarios por servicio en un rango | `org_daily_usage_by_service` | `org_id` | `usage_date DESC`, `service` | un rango dentro de una partición |
| Q2, top-N de servicios por costo en 14 días | `org_daily_usage_by_service` | `org_id` | `usage_date DESC`, `service` | 14 días de una partición (84 filas como máximo), suma y orden en el cliente |

Una partición tiene como máximo 6 filas por día, así que crece
a unas 2.190 filas por año sin importar el tamaño de la organización, y no hace falta un bucket por
mes (D11). La carga compara la cantidad de filas escritas con las de Gold. Las 11.050 filas
coinciden.

Q1 para `org_okep7y6w` del 1 al 7 de agosto, como está en `cql/q1_daily_cost_by_service.cql` y
como lo devuelve cqlsh (primeras filas de 25, `serving_q1_cqlsh.txt`):

```text
SELECT usage_date, service, daily_cost_usd, requests, events
FROM cloud_analytics.org_daily_usage_by_service
WHERE org_id = 'org_okep7y6w'
  AND usage_date >= '2025-08-01' AND usage_date <= '2025-08-07';

 usage_date | service    | daily_cost_usd | requests | events
------------+------------+----------------+----------+--------
 2025-08-07 |    compute |       191.8968 |      212 |      3
 2025-08-07 | networking |         3.8985 |      378 |      6
 2025-08-06 | networking |         0.3809 |     null |      3
...
(25 rows)
```

Q2 es la misma lectura con `usage_date` del 18 al 31 de agosto: cqlsh devuelve las 54 filas de la
partición (`serving_q2_cqlsh.txt`) y el cliente las suma y ordena (`serving_q2_driver_astra.txt`):

```text
rank | service  | cost_14d_usd | days_with_usage
-----+----------+--------------+----------------
1    | genai    | 211.0267     | 10
2    | storage  | 117.8743     | 13
3    | database | 90.8801      | 9
```

El `null` de `requests` es un día sin requests medidos, no un cero (D6).

AstraDB se probó con la misma carga, cambiando solo la conexión: `SERVING_TARGET=astra`, el secure
connect bundle por ruta, el usuario `token` y el token como contraseña, todo por variables de entorno
y fuera del repo. El keyspace `cpa` se creó desde la consola, porque AstraDB no admite
`CREATE KEYSPACE`, y las tablas salen del mismo `cql/01_tables.cql`. Cargamos Gold dos veces:

| Tabla en AstraDB | Primera carga | Segunda carga | Gold |
| :--- | ---: | ---: | ---: |
| `cpa.org_daily_usage_by_service` | 11.050 | 11.050 | 11.050 |

Q1 y Q2 devuelven sobre AstraDB los mismos resultados que en local
(`serving_q1_driver_astra.txt`, `serving_q2_driver_astra.txt`, `astra_load_1.log`). En la evidencia copiada, el token, el host de
la base y las rutas locales quedan enmascarados.

## 6. Analítica: anomalías de costo

`cost_anomaly_mart` compara el costo de cada día con los 28 días previos de la misma organización y
servicio, con el z-score modificado `0,6745 × (x − mediana) / MAD`. Si MAD es 0, cae a la desviación
absoluta media. Un día con menos de 14 días de historia no se puntúa. El corte es 3,5. El detalle y
las alternativas están en D12.

| Medida | Valor |
| :--- | ---: |
| filas puntuadas | 7.043 de 11.050 (63,7%) |
| días marcados | 180 (2,56% de los puntuados): 138 picos y 42 caídas |
| organizaciones con al menos un día marcado | 69 de 80 |
| caso más alto | `org_okep7y6w`, compute, 2025-08-07: 191,90 USD contra una mediana de 11,27 (score 106,5) |

El método es relativo a cada serie: un día caro para una organización que siempre gasta mucho no se
marca. La limitación principal son los días con menos de 14 días de historia, que quedan sin puntuar.

Elegimos un método estadístico y no un modelo de Spark ML porque la mediana y el MAD casi no se
mueven con la cola larga de los costos y cada alerta se explica contra la historia de su propia
serie. Un pipeline de Spark ML queda como deseable para el final (§10, D12).

## 7. Idempotencia

`make rerun-evidence` resetea el lake y el keyspace, corre el pipeline completo dos veces seguidas y
guarda los conteos en `counts_run_1.json` y `counts_run_2.json`. Las dos tienen las mismas filas.

| Mecanismo | Qué protege | Resultado de la segunda corrida |
| :--- | :--- | :--- |
| registro de ingestados | Landing a Bronze, batch | los 7 CSV salteados, Bronze sin cambios |
| checkpoints de las dos queries | Landing a Bronze, streaming | 0 archivos nuevos, los mismos commits que en la primera: 13 en Bronze (12 con datos y uno vacío que avanza el watermark) y 12 en Quarantine |
| reconstrucción y dynamic partition overwrite | Silver y Gold | mismas filas en las 18 tablas del lake |
| Quarantine de reglas sobrescrita entera | Quarantine | sin copias del día anterior |
| upsert por primary key | Cassandra | las mismas 11.050 filas |

## 8. Pruebas, configuración y errores

`make test` corre 39 pruebas con Spark local y sin Cassandra: esquemas sobre la muestra, carga batch
(cuarentena de una fila mala, registro, encabezado que no coincide), streaming (separación antes del
dedupe, nada perdido por el watermark, re-ejecución vacía, un `:Zone.Identifier` en la fuente), cada regla de calidad sobre los
fixtures, Silver de punta a punta, la semántica de nulos de Gold y su conciliación, las anomalías
(MAD, caída a la desviación media, historia mínima, ventana que solo mira hacia atrás) y el parseo
de los CQL y la sal obligatoria del hash de emails.

La configuración vive en `config/pipeline.env`, copiado de `config/pipeline.env.example`, y cada
variable se puede pisar desde el entorno. La evidencia se generó con Java 21. Los errores esperables (otra
versión de Java, `PII_HASH_SALT` sin definir, Landing incompleto, encabezado CSV distinto del
esquema, Gold que no concilia, Cassandra o AstraDB inaccesibles o sin credenciales) cortan la
corrida con un mensaje en el log y código 1.

## 9. Gobierno y ejecución

La matriz de gobierno está en `docs/governance.md` y se incluye al final de este documento.

## 10. Backlog para el MVP final

| Prioridad | Trabajo |
| :--- | :--- |
| Obligatorio | marts `revenue_by_org_month`, `tickets_by_org_date` y `genai_tokens_by_org_date` en Gold, con sus tablas en Cassandra y las consultas Q3, Q4 y Q5 |
| Obligatorio | confirmar con la cátedra si Q3 a Q5 son por organización o globales (§8.1 del diseño) |
| Obligatorio | cargar en AstraDB los marts nuevos del final, con sus consultas |
| Obligatorio | autenticación y roles en Cassandra: `PasswordAuthenticator`, un rol de carga con `MODIFY` y uno de lectura con `SELECT` (seguridad, §7.2.7 y §9.3 de la consigna) |
| Obligatorio | alertas sobre el resumen JSON de cada corrida (etapa fallida, O3, conciliación de Gold) y un tablero del tamaño de Quarantine (observabilidad, §7.2.7 y §9.3) |
| Obligatorio | presentación ejecutiva, video y guion de la defensa |
| Deseable | vista intradía provisoria del speed layer con `foreachBatch` (D3, apéndice C) |
| Deseable | job de retención para Bronze (90 días) y Quarantine (180 días) |
| Deseable | Silver incremental por las fechas de evento que tocó cada corrida, en lugar de reconstruir todo |
| Deseable | pipeline de Spark ML con KMeans para segmentar organizaciones por perfil de uso (requests, cómputo, almacenamiento, GenAI, costo), con su métrica de silueta |
| Fuera de alcance | Kafka u otro broker, Delta o Iceberg, Declarative Pipelines, Real-Time Mode, `transformWithState` |
