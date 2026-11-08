# Decisiones

Decisiones de diseño, con las alternativas que miramos y lo que cuesta cada una. D1 a D10 vienen de
la entrega 1. D11 y D12 quedaron abiertas en la entrega 1 y se cierran con los datos de Gold. D13 a
D17 son las que trajo la implementación. Fecha: 2026-11-08.

Las mediciones salen de [`evidence/landing_profile.md`](evidence/landing_profile.md). Documento de
diseño: [`docs/design.md`](docs/design.md).

| # | Decisión | Estado |
| :-- | :--- | :--- |
| D1 | Un único esquema explícito que cubre las dos versiones de evento | Aceptada |
| D2 | Leer `value` como string en Bronze y castear en Silver con `try_cast` | Aceptada |
| D3 | Híbrido estilo Lambda, con un speed layer sin ventanas | Aceptada. Entrega 2: batch layer más ingesta streaming a Bronze; speed layer diferido, latencia de Gold D+1 |
| D4 | Cinco zonas, Parquet con Snappy | Aceptada |
| D5 | Una sola columna de fecha, `ingest_date` en Bronze y `event_date` en Silver | Aceptada |
| D6 | Política de calidad: primero imputar | Aceptada |
| D7 | Acción por fuente, y dos supuestos de escala | Aceptada |
| D8 | Landing inmutable, idempotencia por sobreescritura y upsert | Aceptada |
| D9 | Convertir moneda por factura, y forzar USD a 1,0 | Aceptada |
| D10 | Las muestras de quarantine salen de fixtures en la entrega 2 | Aceptada |
| D11 | Modelo query-first en Cassandra: una partición por organización, días como clustering | Aceptada |
| D12 | Anomalías de costo: z-score modificado con MAD sobre una ventana de 28 días, corte 3,5 | Aceptada |
| D13 | Carga batch: registro de ingestados, y el dedupe por clave va en Silver | Aceptada |
| D14 | Streaming: `availableNow` para corridas y demo, una query y un checkpoint por destino | Aceptada |
| D15 | Quarantine en cuatro tablas, una por escritor | Aceptada |
| D16 | Carga a Cassandra con el driver de Python desde `foreachPartition`, local y AstraDB | Aceptada |
| D17 | `users.email` se reemplaza en Silver por un hash con sal y el dominio | Aceptada |

## D1. Un único esquema explícito que cubre las dos versiones de evento

En Landing el mismo evento llega con tres formas: 10.800 filas con 11 campos, 29.268 con 12 y 3.132
con 13, porque `schema_version=2` agrega `carbon_kg` y los eventos v2 de GenAI agregan
`genai_tokens`. Declaramos un `StructType` que cubre la unión de los 13, con `carbon_kg` y
`genai_tokens` nullable, y lo usamos en todas las lecturas.

La alternativa que descartamos es la inferencia, que muestrea los archivos que le toquen: un
micro-batch con solo eventos v1 inferiría un conjunto de columnas más angosto y el esquema cambiaría
entre corridas. Un esquema por versión más una unión serían dos lectores y un paso de merge, sin
ganar nada frente a columnas nullable.

Las filas v1 quedan entonces con dos columnas nulas, que es exacto y no una pérdida, y un campo nuevo
en una v3 significa editar una sola declaración. Verificado sobre los 120 archivos: 43.200 filas y
ningún registro corrupto.

## D2. Leer `value` como string en Bronze y castear en Silver con `try_cast`

Contexto. `value` viene como número JSON 41.014 veces, como string entre comillas 1.309 veces, y nulo
877 veces.

Decisión. En Bronze `value` es string. Silver castea con `try_cast`, y un cast que falla es una regla
de rechazo.

Alternativas. Castear en la lectura deja que Spark convierta un valor imposible de parsear en un nulo
silencioso, que termina dentro de una suma de costos sin que nada avise.

Consecuencias. Bronze no se puede sumar directo sobre `value`, lo cual está bien porque Bronze no es
una capa de consulta. Hace falta `try_cast` y no `cast`: Spark 4 viene con el modo ANSI SQL activado,
y un cast común levanta excepción y cortaría el job en lugar de mandar la fila a Quarantine.

## D3. Híbrido estilo Lambda, con un speed layer sin ventanas

Contexto. El §2.1 de la consigna pide métricas de uso, consumo y costo incremental en near real-time,
y además maestros en batch. Cada uno de los 120 archivos abarca 59 de los 60 días del dataset, y el
orden de llegada casi no tiene relación con el tiempo del evento.

Decisión. Un speed layer y un batch layer sobre los mismos eventos. El job de streaming hace append a
Bronze y, con `foreachBatch`, mantiene una vista intradía provisoria de costo y requests por
organización, servicio y fecha de evento. No guarda ventanas: agrega dentro de un micro-batch, y la
idempotencia sale de incluir el `batchId` de Spark en la clave de Cassandra: reprocesar un batch
sobreescribe su propia fila. El batch layer recalcula todos los marts de Gold desde Silver y en D+1
reemplaza los números provisorios por los definitivos. Los maestros y la facturación son solo batch.

Alternativas. Batch puro incumple el requisito de near real-time y la capacidad obligatoria de
Structured Streaming del §4.4. Kappa puro falla por dos lados: los maestros son snapshots periódicos
y la facturación es mensual, sin ninguna necesidad de latencia, y calcular el Gold diario en streaming
o descarta entre el 90% y el 97,5% de los eventos con un watermark de un día (según cuántos archivos
entren en cada micro-batch, `stream_bronze.md`), o necesita un watermark de 60 días manteniendo
estado de todo el período. Un streaming que solo escribe Bronze resuelve la ingesta continua, pero no
da costo ni requests antes de la corrida batch, y esa parte del §2.1 queda sin cubrir mientras no
exista el speed layer. Una agregación con estado en lugar de `foreachBatch` tendría que guardar 60
días de ventanas para no perder eventos tardíos.

Consecuencias. Dos caminos de ejecución para operar, y costo y requests se calculan en los dos, así
que pueden no coincidir entre el último micro-batch y la siguiente corrida de batch. Solo esas dos
medidas están duplicadas, el número provisorio se sirve marcado como provisorio, y en D+1 gana el
valor del batch. Mantener las dos definiciones alineadas es trabajo manual, y por eso el speed layer
se limita a dos medidas. Los marts finales están tan frescos como la agenda del batch, O1.

Estado en la entrega 2. Está implementado el batch layer completo (Silver, Gold, anomalías y la
carga a Cassandra) y la ingesta streaming a Bronze y Quarantine, con watermark, dedupe y
checkpoints. La vista intradía provisoria del speed layer está diferida al final, como deseable del
backlog. Hasta que exista, la latencia de Gold y de Cassandra es D+1: un evento que entra a Bronze
llega al mart en la próxima corrida batch.

## D4. Cinco zonas, Parquet con Snappy

Contexto. La consigna exige Landing, Bronze, Silver y Gold más quarantine en Parquet. Las filas malas
necesitan un lugar que no sea ni descartarlas ni dejarlas en los marts.

Decisión. Cinco zonas, cada una con un solo tipo de escritor, un formato declarado y una condición de
promoción explícita. Quarantine es una zona par que ningún mart lee. Parquet con Snappy en todas las
zonas que administramos.

Alternativas. Una columna booleana `is_valid` en Silver obliga a cada consulta de aguas abajo a
acordarse de filtrarla, y la que se olvide corrompe un mart. CSV y JSON no dan poda de columnas ni
pushdown. Gzip no es divisible. Delta e Iceberg darían sobreescritura atómica de particiones pero
quedan fuera del stack pedido.

Consecuencias. Un camino más para escribir y monitorear. En cambio, la validez pasa a ser una
propiedad de la zona donde está la fila y no un predicado que cada consumidor repite.

## D5. Una sola columna de fecha, `ingest_date` en Bronze y `event_date` en Silver

Contexto. 60 días de eventos, unos 720 por día. Una escritura de streaming produce al menos un
archivo por micro-batch y partición tocada, y cada archivo de la fuente abarca entre 59 y 60 fechas de
evento distintas.

Decisión. Particionar por una sola columna de fecha, dejando `service` y `region` como columnas.
Bronze usa `ingest_date` para los eventos y para los seis maestros que llegan como snapshot, y
`month` para la facturación. Silver usa `event_date`. Gold particiona por `usage_date`, que es la
misma fecha del evento con el nombre que le da el grano del mart (`org_id`, `usage_date`,
`service`). Silver y Gold se reconstruyen en batch con `coalesce` por partición. La retención de
Bronze se mide sobre `ingest_date`.

Alternativas. Agregar `service` y `region` da 2.520 particiones de unas 17 filas, o sea archivos de
alrededor de 1 KB, donde manda el costo de metadata por archivo y el scheduling de una tarea por
archivo. Particionar Bronze por `event_date` escribe 7.180 archivos de 6 filas, y `coalesce` no lo
arregla porque cada micro-batch escribe en todas esas particiones a la vez. Particionar Bronze por `event_date` con
un job de compactación posterior agrega un job cuyo único objetivo es deshacer una elección que somos
libres de no tomar.

Consecuencias. En los maestros, cada snapshot queda en la partición del día en que se cargó: la
retención de 90 días borra directorios enteros, un snapshot nuevo no se mezcla con el anterior y
Silver se queda con la última fila por clave (D13). Las consultas por rango de fechas podan
particiones y los filtros por servicio se
apoyan en las estadísticas de row group. Consultar Bronze por tiempo de evento requiere un scan
completo, que es aceptable porque Bronze no es una capa de consulta. Una retención de Bronze medida
sobre `event_date` ya habría vencido todas las filas de este dataset de 2025. Esto también es un
segundo argumento para D3: la capa de streaming no puede producir particiones por tiempo de evento con
un tamaño de archivo sano. Hay que revisarlo en los 1,6 TB de Parquet por día proyectados, donde el
segundo nivel debería ser `hour` o un bucket por hash de `org_id`.

## D6. Política de calidad: primero imputar

2.075 eventos llegan con `unit` nulo y se llevan el 4,92% de todo el costo. En los datos `metric`
determina `unit` uno a uno sin ninguna contradicción, y otros 877 eventos tienen `value` nulo por un
2,01% del costo.

Decisión. A Quarantine van solo las contradicciones: `metric` desconocido, un `unit` que no coincide
con su `metric`, un `value` que no castea, un `event_id` nulo, un `timestamp` nulo, un `org_id` o
`resource_id` que no joinea, y un `resource_id` que existe pero pertenece a otra organización
(`resource_org_mismatch`), que es el mismo caso de dos claves que se contradicen. Un campo que falta pero no es ambiguo se repara y se marca: `unit` se imputa desde
`metric` con `unit_imputed = true`. Un `value` nulo conserva su costo y aporta null, no cero, a las
sumas de uso.

Alternativas. Mandar a Quarantine todo `unit` nulo rechaza el 4,92% de todo el costo y subestima cada
mart de FinOps en torno al 5%. Imputar en silencio hace que una reparación no se distinga de un dato
de origen. Tratar un `value` nulo como cero sesga todos los promedios para abajo e inventa una
medición que nunca se tomó. Descartar esas filas tira el 2,01% del costo real.

Consecuencias. La base de Quarantine para eventos es 0 de 43.200, que es contra lo que está fijado O3.
La imputación vale mientras `metric` determine `unit`, y el notebook lo verifica antes de
confiar en eso. Los agregados de uso y de costo pueden tener denominadores distintos para el mismo
día de una organización, por lo que una métrica de uso tiene que ir con su conteo de filas.

## D7. Acción por fuente, y dos supuestos de escala

Contexto. Los maestros tienen defectos de distinta naturaleza y tratarlos todos igual sería un error
en los dos sentidos.

Decisión. Tres acciones, elegidas según si lo que no es confiable es la fila o solo un campo.

| Acción | Cuándo aplica | Reglas |
| :--- | :--- | :--- |
| quarantine | la fila se contradice y no vale nada sin el campo roto | `support_tickets.resolved_at` anterior a `created_at`, hoy 0 filas |
| reparar o anular, más un flag | un campo está mal pero tiene un valor conocido o irrelevante | tasa USD a 1,0 (160), `csat` fuera de 1 a 5 (40), `nps_score` fuera de -100 a 100 (1), `credits` nulo a cero (137) |
| solo marcar | el valor es plausible, o la fila sirve igual | las dos reglas de timestamps de `users` (232 y 249), `subtotal` negativo (13), `converted` sin `clicked` (96) |

Los timestamps de `users` se quedan. Las dos reglas se disparan en el 29,0% y el 31,1% de las filas y
462 de 800 rompen al menos una, cuando un dataset generado de forma causal mostraría cerca de 0%, así
que los tres timestamps se sortearon de forma independiente. Quedan marcados como de baja confianza y
fuera de cualquier métrica de antigüedad o de recencia. Los campos de identidad siguen siendo usables.

Supuestos. La consigna no fija ninguna de las dos escalas, de modo que son nuestras: `csat` es de 1 a 5, y
los datos traen 0, 6 y 7; las dos columnas de NPS usan la escala agregada de -100 a 100, porque
`nps_surveys.nps_score` va de -16 a 68 y eso descarta una escala de 0 a 10 por respondente. Si la
cátedra tiene otras escalas en mente, cambian dos reglas.

Alternativas. Mandar a Quarantine las filas contradictorias de `users` tira el 57,8% de una dimensión
cuyos campos de identidad están sanos, y parte al medio cualquier conteo de usuarios por organización.
Anular los timestamps es arbitrario, porque nada dice cuál de los tres está mal. Marcar sin la nota de
baja confianza no evita que alguien calcule antigüedad promedio de cuenta sobre una columna que no lo
soporta.

Consecuencias. La base de Quarantine para los maestros es 0 de 4.112 filas. Junto con D6, los datos
reales no rechazan nada, y de ahí sale D10: el camino de rechazo queda sin ejecutar.

## D8. Landing inmutable, idempotencia por sobreescritura y upsert

Contexto. La consigna exige que los datos crudos queden intactos. `event_id` no tiene duplicados en
los datos, pero una re-ejecución vuelve a leer los mismos archivos, y los duplicados son una
propiedad de la ejecución y no de la fuente.

Decisión. Nada escribe en Landing, y reprocesar significa volver a leer los archivos originales.
Silver y Gold sobreescriben la partición afectada en lugar de hacer append. La carga a Cassandra hace
upsert por la clave de cada mart, y la vista intradía provisoria usa el `batchId` en la clave (D3). Un
registro de archivos ya ingestados controla el paso de Landing a Bronze.

El dedupe de streaming corre después de separar las fallas de parseo, no antes: una fila que no
parsea llega con `event_id` nulo y `dropDuplicates` agrupa los nulos entre sí, de manera que
deduplicar primero dejaría una sola fila corrupta por micro-batch y perdería el resto antes de que
llegue a Quarantine. Y usa un watermark sobre `ingest_ts`, nunca sobre `timestamp`. Las dos APIs de
dedupe descartan datos más allá del watermark: PySpark 4.2 documenta que `dropDuplicates` tira "data
older than watermark to avoid any possibility of duplicates", y que `dropDuplicatesWithinWatermark`,
agregada en Spark 3.5, tira "too late data older than watermark". Como `ingest_ts` se sella en el
momento de la lectura, nunca queda atrasado respecto de la llegada: nada es tardío contra él y Bronze
conserva todos los eventos. El watermark acota solo cuánto tiempo se recuerda un `event_id`
duplicado, en tiempo de llegada, que es el horizonte en el que vive un reintento. La deduplicación
exacta es tarea de la reconstrucción batch, que deduplica una partición `event_date` entera desde
Bronze sin ningún watermark.

Alternativas. Corregir filas dentro de Landing destruye la única base reproducible. Hacer append y una
pasada de dedupe después hace que la corrección dependa de que un job de limpieza haya corrido, y la
ventana entre los dos es visible para los consumidores. Sobre la columna del watermark, uno por tiempo
de evento de 1 a 7 días borraría entre el 81% y el 90% de los eventos de Bronze con los micro-batches
de 10 archivos que usamos, y entre el 88% y el 97,5% con un archivo por micro-batch
(`stream_bronze.md`), y Bronze es la zona cuyo trabajo es ser una copia fiel. Uno de 60 días los
conservaría pero mantendría 60 días de `event_id` en estado:
trivial con las 43.200 claves de hoy, y 2,6 billones (2,6 × 10¹²) en la proyección del §2.1.

Consecuencias. Landing no se vence nunca, lo que cuesta almacenamiento, y a cambio Bronze se puede
guardar solo 90 días. Volver a correr cualquier fecha es seguro. La sobreescritura de particiones
no es atómica en object storage común, y un lector puede ver por un momento una partición a
medio escribir, algo aceptable con una cadencia diaria y la razón por la que D4 menciona Delta e
Iceberg como lo que habría que adoptar si deja de serlo.

## D9. Convertir moneda por factura, y forzar USD a 1,0

Contexto. `billing_monthly.csv` tiene 240 facturas en USD (160), ARS (51) y EUR (29), cada una con su
propio `exchange_rate_to_usd`. Las 160 facturas en USD traen una tasa distinta de 1,0, entre 0,855 y
1,118. Hay 13 facturas con subtotal negativo y 137 con `credits` nulo.

Decisión. Normalizar cada factura con su propia tasa antes de cualquier suma. Cuando `currency` es
USD, forzar la tasa a 1,0 y poner `fx_overridden = true`. Tratar un `credits` nulo como crédito cero.
Dejar los subtotales negativos como ajustes reales y marcarlos.

Alternativas. Una tasa por mes deforma el revenue de toda factura cuya tasa difiera del promedio.
Confiar en la tasa USD hace que el revenue en USD de una factura en USD dependa de un número sin
sentido. Mandar a Quarantine las 160 facturas en USD tira el 67% de la facturación y todo el revenue
en USD por un solo campo con valor conocido. Descartar los subtotales negativos infla el revenue.

Consecuencias. Forzar la tasa mueve el revenue total apenas un 0,07%, de 164.184,90 a 164.293,06 USD,
porque las tasas se reparten de forma simétrica alrededor de 1,0 y se cancelan entre las 160 facturas.
Por factura el error va de -14,5% a +11,8%, y Q4 sirve una fila por organización y mes. Una
conciliación sobre el total habría pasado mientras dos tercios de las filas servidas estaban mal, y
por eso la conciliación se chequea por organización y día y no sobre el total. La corrección queda visible en
`fx_overridden`.

## D10. Las muestras de quarantine salen de fixtures en la entrega 2

Contexto. Después de D6 y D7 ningún registro real queda rechazado, con lo cual el camino de escritura
a Quarantine, la columna `rule_name` y la separación entre filas que pasan y filas rechazadas no tienen
entrada.

Decisión. La entrega 2 agrega un conjunto chico de fixtures sintéticos: un registro por regla de
rechazo, más un registro de control que parece defectuoso y tiene que pasar, con `unit` nulo y `value`
como el texto `"4.25"`. El registro de control está porque un conjunto de reglas puede fallar en los
dos sentidos, y D6 midió lo que cuesta ser demasiado estricto. Las muestras de quarantine que pide la
entrega 2 salen de esos fixtures más lo que generen los datos reales hasta entonces.

Alternativas. Inyectar filas malas en Landing rompe D8 y corrompe todas las bases de `evidence/`.
Esperar datos malos reales entrega el camino sin haberlo ejecutado nunca. Relajar las reglas para que
algunas filas reales se rechacen es ajustar las reglas a la prueba, y D6 midió lo que eso cuesta.

Consecuencias. Los fixtures son datos de prueba y viven en `tests/fixtures/landing/`, con el mismo
layout que Landing: `make fixtures` corre el pipeline con `DATA_LANDING` apuntando ahí y un lake
propio en `datalake/_fixtures/`. No entran nunca a `data/landing/` ni se cuentan en un perfil o en
un mart. La regla más difícil de ejercitar sin ellos es la del `value` que no
castea, que además es la que depende de `try_cast` en lugar de `cast`: ver D2.

## D11. Modelo query-first en Cassandra: una partición por organización, días como clustering

Contexto. Q1 pide costo y requests diarios por servicio de una organización en un rango de fechas.
Q2 pide el top-N de servicios por costo de una organización en los últimos 14 días. Los conteos
reales de Gold: 11.050 filas en 80 organizaciones, entre 33 y 286 filas por organización en 60
días, y como mucho 6 filas por organización y día, una por servicio.

Decisión.

| Consulta | Tabla | Partition key | Clustering columns | Lectura |
| :--- | :--- | :--- | :--- | :--- |
| Q1 | `org_daily_usage_by_service` | `org_id` | `usage_date DESC`, `service ASC` | un rango de `usage_date` dentro de una partición |
| Q2 | `org_daily_usage_by_service` | `org_id` | `usage_date DESC`, `service ASC` | los últimos 14 días de una partición (84 filas como máximo), suma y orden en el cliente |
| Q6 | `cost_anomalies_by_org` | `org_id` | `usage_date DESC`, `service ASC` | un rango de días de una partición, el cliente se queda con `is_anomaly` |

Ninguna consulta usa índices secundarios ni `ALLOW FILTERING`, y cada una toca una sola partición.
Q2 suma en el cliente porque CQL solo agrupa por un prefijo de la primary key y no ordena por una
columna regular. Sobre 84 filas eso no cuesta nada. `cost_anomaly_mart` va a su propia tabla, con
todos los días puntuados y no solo los marcados: si cambia el corte, la recarga sobrescribe las
mismas claves y no quedan filas viejas.

Tamaño de partición. La partición crece con los días y no con el tamaño de la organización, porque
el grano tope es 6 servicios por día: 2.190 filas por año en el peor caso, unas 300 KB con filas
de unos 150 bytes, lejos de los 100 MB que se usan como límite práctico. Diez años serían unas
22.000 filas. No hace falta un bucket por mes, y agregarlo obligaría a Q1 y Q2 a leer una o dos
particiones según dónde caiga el rango.

Replicación y consistencia. Local, un nodo en el datacenter `dc1` con `NetworkTopologyStrategy` y
factor 1 (`cql/00_keyspace.cql`). En producción sería `{'dc1': 3}` y `LOCAL_QUORUM` para leer y
escribir: dos de tres réplicas, así que una lectura siempre ve la última carga (2 + 2 > 3) y el
sistema tolera la caída de un nodo. El pipeline escribe con `LOCAL_QUORUM` (configurable con
`CASSANDRA_CONSISTENCY`), que con un nodo es una réplica. En AstraDB la replicación la fija el
servicio y las escrituras tienen que ser `LOCAL_QUORUM`.

Alternativas. Una tabla con `service` antes que `usage_date` en el clustering permitiría agrupar
Q2 por servicio en el servidor, pero el rango de fechas de Q1 dejaría de ser contiguo y habría que
mantener dos copias del mart. Particionar por `(org_id, mes)` acota algo que ya está acotado.
Particionar por fecha concentraría todas las escrituras del batch diario en una partición caliente.

Consecuencias. Q3, Q4 y Q5 siguen supuestas por organización, como dice §8.1 del diseño. Si alguna
resulta global, va a necesitar su propia tabla particionada por fecha. 80 organizaciones son 80
particiones repartidas por hash, así que la carga queda pareja entre nodos.

## D12. Anomalías de costo: z-score modificado con MAD sobre una ventana de 28 días, corte 3,5

Contexto. El método tenía que ser relativo, porque el mismo monto es normal para una organización
grande y raro para una chica, y se elegía mirando la serie diaria de Gold. Esa serie tiene 11.050
filas en 262 combinaciones de organización y servicio. Cada combinación tiene uso entre 24 y 60 de
los 60 días (mediana 39). El costo diario va de -134,24 a 340,53 USD, con p50 en 8,78 y p99 en
63,87, y 35 días tienen costo total negativo.

Decisión. `cost_anomaly_mart` tiene el grano de `org_daily_usage_by_service` y, para cada fila,
compara el costo del día con los días anteriores de la misma organización y servicio:

- Historia: los días con uso dentro de los 28 días previos, sin incluir el día evaluado. Un día con
  menos de 14 días de historia no se puntúa (`score_method = insufficient_history`).
- Score: `0,6745 × (x − mediana) / MAD`, el z-score modificado de Iglewicz y Hoaglin. Si MAD es 0,
  porque más de la mitad de la historia es un mismo valor, se usa la desviación absoluta media
  escalada por 1,2533 (`mean_ad`). Si las dos son 0, no hay score (`constant_history`).
- Corte: `|score| > 3,5` marca `is_anomaly`, con `anomaly_direction` igual a `spike` o `drop`.

Medido sobre Gold, se puntúa el 63,7% de las filas (7.043). El |score| tiene p50 0,74, p95 2,67,
p99 5,47 y p99,9 22,36:

| Corte | Días marcados | % de los puntuados |
| :--- | ---: | ---: |
| 3,0 | 262 | 3,72% |
| 3,5, elegido | 180 | 2,56% |
| 5,0 | 86 | 1,22% |
| 7,0 | 52 | 0,74% |

Con 3,5 salen 138 picos y 42 caídas en 69 organizaciones, unas 4 a 5 alertas por día para toda la
cartera. El caso más alto es `org_okep7y6w`, compute, el 2025-08-07: 191,90 USD contra una mediana
de 11,27 y un MAD de 1,14. Dejamos el 3,5 de la literatura porque el volumen de alertas se puede
revisar a mano. `ANOMALY_THRESHOLD`, `ANOMALY_WINDOW_DAYS` y `ANOMALY_MIN_HISTORY` lo cambian sin
tocar código.

Alternativas. La media y el desvío estándar los mueven los mismos picos que tienen que encontrar.
Los percentiles por serie necesitan muchos más días que los 24 a 60 que hay por combinación. Una
línea de base sobre los 60 días completos puntúa más filas, pero usa días posteriores al evaluado y
no se puede correr igual cada mañana en D+1. Un pipeline de Spark ML, por ejemplo un clustering
sobre las features diarias, también podría separar días raros, pero la planificación del curso pide
no forzar ML cuando un método estadístico responde mejor. Acá responde mejor: la mediana y el MAD
casi no se mueven con la cola larga medida en los incrementos (p99 de 16,69 contra un máximo de
317,43), y cada alerta se explica contra la historia de su propia serie, con la mediana y el MAD que
la produjeron. Un cluster, en cambio, dice a qué grupo se parece un día, no por qué es raro para esa
organización. Dejamos un pipeline de Spark ML como deseable para el final: KMeans para segmentar
organizaciones por perfil de uso, que es una pregunta de Producto y no de anomalías.

Chequeo de detección. Sin etiquetas, la referencia son los días que un umbral fijo llamaría
extremos (`detection_check` en `evidence/entrega-2/cost_anomaly_summary.json`):

| Días | Total | Marcados | Puntuados sin marcar | Sin historia |
| :--- | ---: | ---: | ---: | ---: |
| costo diario por encima del p99 de Gold (63,91 USD) | 110 | 39 (35,5%) | 41 | 30 |
| costo diario negativo | 35 | 4 (11,4%) | 15 | 16 |

Los picos marcados son saltos contra la propia serie, como `org_g8sbi4q2`, database, el 2025-08-05:
142,95 USD contra una mediana de 9,22. Los días caros sin marcar son de series que gastan mucho
siempre: `org_ktakpuxq`, compute, el 2025-08-06, gastó 108,76 USD contra una mediana de 35,58 (score
2,92). Un umbral fijo los marca porque la organización es grande, y eso es lo que el método relativo
quiere evitar. Los costos negativos casi no se marcan porque la mayoría cae dentro de la variación de
su serie: `org_dhylurtp`, compute, el 2025-07-27, -9,00 USD contra una mediana de 19,52 (score -1,83).
Esos días ya llevan sus eventos con `cost_anomaly_flag` y su conteo en `negative_cost_events`, que es
el control pensado para correcciones. El único negativo grande, -134,24 USD en `org_0lvsnujz`, sí
queda marcado.

Consecuencias. El 36,3% de las filas, los primeros días de cada serie, queda sin puntuar, y eso
explica 30 de los 110 días caros y 16 de los 35 negativos que no se marcan. Un día sin uso no tiene
fila en Gold, así que la historia son los días con uso y no se rellena con ceros: el score responde
si el día es raro comparado con los días en que el servicio se usó. Sin etiquetas, la calidad del
método se mide con el chequeo de detección y los ejemplos, no con precision ni recall.

## D13. Carga batch: registro de ingestados, y el dedupe por clave va en Silver

Contexto. La consigna pide deduplicar los maestros en Bronze "cuando aplique", y el diseño (§5) dice
que Bronze conserva el grano de la fuente sin filtrar. Los 7 CSV no tienen claves repetidas, así que
los duplicados posibles son los de una re-ejecución que vuelve a leer el mismo archivo.

Decisión. Bronze es solo append y el registro `datalake/_registry/ingested_files` decide qué entra:
un archivo cuyo nombre ya figura ahí se saltea. El registro guarda además el sha256, el tamaño y los
conteos de cada archivo. Si un archivo registrado cambia de contenido, la corrida lo informa como
warning y no lo carga, porque Landing es inmutable (D8). Bronze cuenta las claves naturales
repetidas de cada archivo y las deja pasar. El dedupe por clave lo hace Silver, quedándose con la
fila del `ingest_ts` más reciente.

Alternativas. Sobrescribir cada maestro en Bronze en cada corrida borra la historia de snapshots,
que es lo único que permitiría reconstruir un cambio de un maestro. Deduplicar por clave en Bronze
elige una fila sobre otra con una regla de negocio, cosa que el diseño reserva para Silver.

Consecuencias. Una re-ejecución no escribe nada en Bronze y lo deja registrado en el log. El
registro se escribe al final del paso: si la corrida se corta entre la escritura de Bronze y la del
registro, la siguiente vuelve a cargar esos archivos, y la copia la absorbe el dedupe de Silver.

## D14. Streaming: `availableNow` para corridas y demo, una query y un checkpoint por destino

Contexto. El diseño (§6.3) fija `processingTime="1 minute"`. Con ese trigger la query no termina
nunca, y para un pipeline que se corre de punta a punta, se re-ejecuta para probar idempotencia y se
muestra en una demo, hace falta un trigger que procese lo que hay en Landing y pare.

Decisión. `STREAM_TRIGGER=available_now` es el valor por defecto: la query consume todos los
archivos pendientes en micro-batches de `maxFilesPerTrigger` (10 archivos, 3.600 eventos) y se
detiene. `STREAM_TRIGGER=processing_time` deja la misma query corriendo con
`processingTime="1 minute"`, que es el modo continuo del diseño. La lógica no cambia entre los dos.
La fuente lee solo `*.jsonl` (`pathGlobFilter`): un archivo copiado desde Windows trae al lado un
`:Zone.Identifier`, y sin el filtro la query lo intenta leer como JSON y falla con `FAILED_READ_FILE`.

Hay dos queries sobre la misma fuente, cada una con su `queryName` y su checkpoint propio:
`bronze_usage_events` y `quarantine_usage_events_parse`. Una falla de parseo se clasifica como
`parse_malformed_json` si no se pudo leer ningún campo, o como `parse_type_mismatch` si algún campo
llegó con el tipo equivocado (por ejemplo, un `timestamp` que no es fecha).

Una fila que parsea pero llega sin `event_id` no se puede deduplicar por `event_id`, porque los
nulos se agrupan entre sí. La clave del dedupe es entonces `event_id` o, si falta, un hash del
contenido de la fila. Así colapsan las copias idénticas y sobreviven las filas distintas, hasta que
Silver las manda a Quarantine por `event_id_null`.

Exactly-once en Bronze. La fuente es re-leíble (archivos en un directorio que nadie modifica), el
checkpoint guarda qué archivos entraron en cada batch y el sink de archivos confirma cada batch en
su log `_spark_metadata`. Un batch que se corta a mitad se rehace entero, y los lectores solo ven lo
confirmado. Hacia Cassandra la garantía es el upsert por la clave del mart (D8).

Qué rompe un checkpoint. Cambiar la fuente o la ruta de salida de una query, cambiar las columnas
del dedupe o la columna del watermark, o renombrar la query sobre el mismo checkpoint. El
procedimiento es el mismo en todos los casos: `make reset-stream` borra los dos checkpoints y sus
salidas, y la próxima corrida reprocesa desde Landing. Cuesta poco porque Landing es inmutable y
Bronze se puede reconstruir (§8.2 del diseño). Cambiar `maxFilesPerTrigger` o el trigger no rompe
nada.

Alternativas. Una sola query con `foreachBatch` que escriba Bronze y Quarantine comparte el
checkpoint y pierde la garantía del sink de archivos: un reintento del batch escribiría dos veces
salvo que la escritura se haga idempotente a mano. Kafka, Delta, las Declarative Pipelines, el
Real-Time Mode y `transformWithState` quedan fuera del stack de esta entrega.

Consecuencias. Con `availableNow` las 43.200 claves entran en el estado del dedupe en menos de un
minuto, todas dentro de la hora del watermark, así que el estado llega a 43.200 filas. Es el peor
caso, acotado por los eventos que llegan en una hora. La corrida `make stream-state-demo`, con un
watermark de 2 segundos, muestra que el estado se libera sin descartar eventos (evidencia en
`evidence/entrega-2/`).

## D15. Quarantine en cuatro tablas, una por escritor

Contexto. Quarantine recibe filas de tres lugares: el streaming de eventos, la carga batch de los
CSV y las reglas de Silver. El sink de archivos de un stream es dueño de su directorio: escribe un
log `_spark_metadata` y los lectores ignoran cualquier archivo que no figure ahí, así que un writer
batch no puede compartir esa carpeta. Además, las violaciones de reglas se derivan de Bronze igual
que Silver, y se tienen que reconstruir con él.

Decisión. Todas las tablas comparten las columnas `entity`, `rule_name`, `raw_record`,
`source_file`, `quarantined_ts` y `run_id`, y particionan por `quarantine_date`.

| Tabla | Escribe | Modo |
| :--- | :--- | :--- |
| `quarantine/usage_events_parse` | query de streaming, fallas de parseo | append, checkpoint propio |
| `quarantine/masters_parse` | carga batch, fallas de parseo de los CSV | append, protegida por el registro de ingestados |
| `quarantine/usage_events_rules` | Silver, reglas de rechazo de eventos | se reconstruye entera en cada corrida |
| `quarantine/masters_rules` | Silver, reglas de rechazo de maestros | se reconstruye entera en cada corrida |

`raw_record` guarda la línea cruda cuando el parseo falló, y la fila de Bronze en JSON cuando falló
una regla. Así se revisa lo que llegó y no lo que quedó después de una reparación. Las reglas de
eventos se evalúan en orden y una fila lleva solo la primera que rompe.

Alternativas. Una sola tabla con una columna `stage` obliga a mezclar append y reconstrucción sobre
el mismo directorio, y choca con el sink del stream. Reconstruir las tablas de reglas con dynamic
partition overwrite dejaría la copia del día anterior en su partición `quarantine_date`: por eso esas
dos tablas se sobrescriben enteras.

Consecuencias. Para medir O3 hay que sumar las cuatro tablas. La retención de 180 días aplica a las
dos tablas de append; las de reglas siempre reflejan el Bronze actual.

## D16. Carga a Cassandra con el driver de Python desde `foreachPartition`, local y AstraDB

Contexto. El conector oficial (`apache/cassandra-spark-connector`) sigue en la release 3.5.1,
compilada para Spark 3.5, y no tiene versión para Spark 4. El driver de Python
`cassandra-driver` 3.30.1 funciona con Python 3.12 y soporta el secure connect bundle de AstraDB.
La consigna permite cargar con conector o con driver (§4.4).

Decisión. Mantenemos PySpark 4.2.0 y cargamos con el driver desde `foreachPartition`: cada tarea
abre una sesión, prepara un `INSERT` y escribe sus filas con `execute_concurrent_with_args`. Antes
se reparticiona por `org_id`, así cada tarea escribe particiones de Cassandra completas. Un
`INSERT` es un upsert por primary key, así que recargar el mismo mart deja las mismas filas. Al
terminar, la carga compara organización por organización cantidad de filas y costo contra Gold, y
si no coinciden la corrida falla.

El destino principal es Cassandra 5.0 en Docker (`infra/docker-compose.yml`). AstraDB es el segundo
destino, probado con el mismo código: `SERVING_TARGET=astra` cambia solo la conexión (bundle por
ruta, usuario `token` y el token como contraseña, todo por variables de entorno y fuera del repo) y
saltea el `CREATE KEYSPACE`, que AstraDB no admite. El keyspace del proyecto en AstraDB es `cpa`,
creado desde la consola, y se elige con `CASSANDRA_KEYSPACE=cpa`. Las dos cargas de Gold dejaron
11.050 filas en cada tabla y pasaron la verificación por organización. Las consultas se corren con
el driver (`python -m pipeline.queries`) contra cualquiera de los dos destinos, y con cqlsh dentro
del contenedor local, desde los mismos archivos `cql/q*.cql`. Contra Cassandra local el driver usa
`TokenAwarePolicy` sobre `DCAwareRoundRobinPolicy` con `dc1`, el data center del compose, así
`LOCAL_QUORUM` se resuelve ahí.

Entorno (§2.2 de la consigna). Corremos el mismo PySpark 4.2.0 fijado en `requirements.txt`, en modo
`local[4]` sobre WSL2 con JDK 21. Lo validamos con `make check`, las 44 pruebas de `make test` y dos
corridas completas desde un clon limpio, con los mismos conteos. Cassandra corre en Docker y AstraDB
se probó con el mismo código. No lo corrimos en Google Colab.

Alternativas. Bajar a PySpark 3.5 para usar el conector obligaba a instalar un JDK 17 y a cambiar la
versión con la que se generó la evidencia de la entrega 1. Escribir con `foreachBatch` desde el
stream mezclaría el serving con la ingesta, y en D3 el serving sale del batch.

Consecuencias. Un `None` se escribe como null, lo que en Cassandra deja un tombstone. Con miles de
filas es despreciable, y es lo correcto: un día sin requests medidos tiene que leerse null y no 0.
La carga no usa el particionado por token del conector, cosa que a esta escala no importa. Contra
AstraDB la carga tarda más (entre 45 y 60 segundos por tabla, contra unos 2 en local), porque cada escritura
cruza internet con `LOCAL_QUORUM` sobre tres réplicas.

## D17. `users.email` se reemplaza en Silver por un hash con sal y el dominio

Contexto. `users.email` es el único dato personal del dataset, y ninguna de las consultas Q1 a Q6
lo necesita. Lo que sí hace falta es contar y deduplicar usuarios por organización.

Decisión. Bronze conserva el email tal como llega, porque es la copia fiel de la fuente y su acceso
está restringido a ingesta. Silver lo reemplaza por `email_hash = sha256(sal + lower(trim(email)))`
y `email_domain`, y descarta la columna original. La sal sale de `PII_HASH_SALT`, que `make setup`
genera al azar en `config/pipeline.env`, fuera de git. Si falta, la corrida se detiene: un hash
sin sal no protege el dato.

Alternativas. Enmascarar el email (`u***@example.com`) deja ver una parte de la dirección y no sirve
como clave. Un sha256 sin sal se revierte con una lista de emails candidatos, y en este dataset
todos tienen la forma `<user_id>@example.com`. Cifrar con una clave permite volver al email, cosa
que nadie aguas abajo necesita, y agrega una clave más que administrar.

Consecuencias. El hash es estable mientras no cambie la sal: cambiarla cambia todos los
`email_hash` de Silver en la siguiente reconstrucción, que es completa (D8), así que no quedan
mezclados. Quarantine puede traer el email en `raw_record` si una fila de `users` no parsea, y por
eso tiene el mismo acceso restringido que Bronze (`docs/governance.md`).
