# Decisiones

Decisiones de diseño, con las alternativas que miramos y lo que cuesta cada una. Primero las
aceptadas y después las dos que dejamos abiertas a propósito. Fecha: 2026-10-03.

Las mediciones salen de [`evidence/landing_profile.md`](evidence/landing_profile.md). Documento de
diseño: [`docs/design.md`](docs/design.md).

| # | Decisión | Estado |
| :-- | :--- | :--- |
| D1 | Un único esquema explícito que cubre las dos versiones de evento | Aceptada |
| D2 | Leer `value` como string en Bronze y castear en Silver con `try_cast` | Aceptada |
| D3 | Híbrido estilo Lambda, con un speed layer sin ventanas | Aceptada |
| D4 | Cinco zonas, Parquet con Snappy | Aceptada |
| D5 | Una sola columna de fecha, `ingest_date` en Bronze y `event_date` en Silver | Aceptada |
| D6 | Política de calidad: primero imputar | Aceptada |
| D7 | Acción por fuente, y dos supuestos de escala | Aceptada |
| D8 | Landing inmutable, idempotencia por sobreescritura y upsert | Aceptada |
| D9 | Convertir moneda por factura, y forzar USD a 1,0 | Aceptada |
| D10 | Las muestras de quarantine salen de fixtures en la entrega 2 | Aceptada |
| D11 | Modelo query-first de tablas en Cassandra | Abierta |
| D12 | Método y umbral de detección de anomalías | Abierta |

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
o descarta el 97,5% de los eventos con un watermark de un día, o necesita un watermark de 60 días
manteniendo estado de todo el período. Un streaming que se detiene en Bronze y no sirve nada deja el
§2.1 sin respuesta y convierte el diseño en batch con un cargador de streaming, no en Lambda. Una
agregación con estado en lugar de `foreachBatch` tendría que guardar 60 días de ventanas para no
perder eventos tardíos.

Consecuencias. Dos caminos de ejecución para operar, y costo y requests se calculan en los dos, así
que pueden no coincidir entre el último micro-batch y la siguiente corrida de batch. Solo esas dos
medidas están duplicadas, el número provisorio se sirve marcado como provisorio, y en D+1 gana el
valor del batch. Mantener las dos definiciones alineadas es trabajo manual, y por eso el speed layer
se limita a dos medidas. Los marts finales están tan frescos como la agenda del batch, O2. Para la
entrega 1 la vista provisoria es solo diseño y queda en el backlog como trabajo posterior a la
entrega 2, que pide el mart diario.

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
Bronze usa `ingest_date`, Silver y Gold usan `event_date`. Silver se reconstruye en batch con
`coalesce` por partición. La retención de Bronze se mide sobre `ingest_date`.

Alternativas. Agregar `service` y `region` da 2.520 particiones de unas 17 filas, o sea archivos de
alrededor de 1 KB, donde manda el costo de metadata por archivo y el scheduling de una tarea por
archivo. Particionar Bronze por `event_date` escribe 7.180 archivos de 6 filas, y `coalesce` no lo
arregla porque cada micro-batch escribe en todas esas particiones a la vez. Particionar Bronze por `event_date` con
un job de compactación posterior agrega un job cuyo único objetivo es deshacer una elección que somos
libres de no tomar.

Consecuencias. Las consultas por rango de fechas podan particiones y los filtros por servicio se
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
con su `metric`, un `value` que no castea, un `event_id` nulo, un `org_id` o `resource_id` que no
joinea. Un campo que falta pero no es ambiguo se repara y se marca: `unit` se imputa desde
`metric` con `unit_imputed = true`. Un `value` nulo conserva su costo y aporta null, no cero, a las
sumas de uso.

Alternativas. Mandar a Quarantine todo `unit` nulo rechaza el 4,92% de todo el costo y subestima cada
mart de FinOps en torno al 5%. Imputar en silencio hace que una reparación no se distinga de un dato
de origen. Tratar un `value` nulo como cero sesga todos los promedios para abajo e inventa una
medición que nunca se tomó. Descartar esas filas tira el 2,01% del costo real.

Consecuencias. La base de Quarantine para eventos es 0 de 43.200, que es contra lo que está fijado O5.
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
de evento de 1 a 7 días borraría del 87,6% al 97,5% de los eventos de Bronze, la zona cuyo trabajo es
ser una copia fiel, y uno de 60 días los conservaría pero mantendría 60 días de `event_id` en estado:
trivial con las 43.200 claves de hoy, y 2,6 billones (2,6 × 10¹²) en la proyección del §2.1.

Consecuencias. Landing no se vence nunca, lo que cuesta almacenamiento, y a cambio Bronze se puede
guardar solo 90 días. Volver a correr cualquier fecha es seguro, O9. La sobreescritura de particiones
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
por eso O9 se chequea por organización y día y no sobre el total. La corrección queda visible en
`fx_overridden`.

## D10. Las muestras de quarantine salen de fixtures en la entrega 2

Contexto. Después de D6 y D7 ningún registro real queda rechazado, con lo cual el camino de escritura
a Quarantine, la columna `dq_rule` y la separación entre filas que pasan y filas rechazadas no tienen
entrada.

Decisión. La entrega 2 agrega un conjunto chico de fixtures sintéticos: un registro por regla de
rechazo, más un registro de control que parece defectuoso y tiene que pasar, con `unit` nulo y `value`
como el texto `"4.25"`. El registro de control está porque un conjunto de reglas puede fallar en los
dos sentidos, y D6 midió lo que cuesta ser demasiado estricto. Las muestras de quarantine que pide la
entrega 2 salen de esos fixtures más lo que generen los datos reales hasta entonces.

Alternativas. Inyectar filas malas en Landing rompe D8 y corrompe todas las bases de `evidence/`.
Esperar datos malos reales entrega el camino sin haberlo ejecutado nunca. Relajar las reglas para que
algunas filas reales se rechacen es ajustar las reglas a la prueba, y D6 midió lo que eso cuesta.

Consecuencias. Los fixtures van a ser datos de prueba. No entran nunca a `data/landing/` ni se cuentan
en un perfil o en un mart. La regla más difícil de ejercitar sin ellos es la del `value` que no
castea, que además es la que depende de `try_cast` en lugar de `cast`: ver D2.

## D11. Modelo query-first de tablas en Cassandra. Abierta

Lo que ya está decidido. Una tabla por forma de consulta, con la partition key elegida para que cada
consulta toque exactamente una partición, sin índices secundarios y sin `ALLOW FILTERING`. Q1 y Q2
comparten `org_daily_usage_by_service`.

Lo que está abierto. Si `(org_id)` sola alcanza como partition key o si hace falta un bucket por mes
para acotar el crecimiento de la partición; el orden de las clustering columns para el top-N de Q2; si
`cost_anomaly_mart` es su propia tabla o un conjunto de columnas sobre el mart diario. Y si Q3, Q4 y
Q5 son por organización o globales: la consigna solo lo aclara para Q1 y Q2, y una consulta global no
entra por una partición `(org_id)`, sino que necesita su propia tabla particionada por fecha.

Por qué lo postergamos. Depende de los conteos de filas de los marts, que existen recién cuando corre
Gold. Se decide antes de escribir el esquema de Cassandra en la entrega 2.

## D12. Método y umbral de detección de anomalías. Abierta

Lo que ya está decidido. El método tiene que ser relativo y no un umbral fijo de costo, porque el
mismo monto es normal para una organización grande y una anomalía para una chica. Los costos negativos
se marcan, no se descartan.

Lo que está abierto. Cuál de z-score, MAD o percentiles, con qué grano, sobre qué ventana móvil y con
qué corte.

Por qué lo postergamos. La elección necesita la distribución de la serie diaria agregada y no de los
incrementos crudos. Por ahora preferimos MAD porque una cola larga como esta, p99 en 16,69 contra un
máximo de 317,43, casi no lo mueve, pero eso hay que verificarlo contra datos de Gold en lugar de
darlo por sentado. Se decide en la entrega 2 junto con `cost_anomaly_mart`.
