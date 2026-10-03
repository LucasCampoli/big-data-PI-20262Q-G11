# Apéndice

Detalle de apoyo de [`design.md`](design.md). Nada de esto hace falta para seguir el argumento: está
acá para que el documento principal quede corto.

## A. Volumen proyectado

En §2.1 proyectamos el dataset a la escala de un proveedor real. A1 a A5 son entradas que elegimos
nosotros, no mediciones, salvo donde la tabla aclara lo contrario.

| | Supuesto | Este dataset | Proyectado |
| :-- | :--- | :--- | :--- |
| A1 | Organizaciones facturables | 80 | 50.000 |
| A2 | Recursos medidos por organización | 5, medido como 400 / 80 | 200 |
| A3 | Muestras de medición por recurso por día | 1,8, medido como 43.200 / 400 / 60 | 4.320, o sea 3 métricas cada 60 s |
| A4 | Tamaño del evento crudo | 299 B, medido | 300 B |
| A5 | Parquet con Snappy contra JSON crudo | | 8x |

Con `eventos/día = organizaciones x recursos x muestras`:

| Métrica | Este dataset | Proyectado |
| :--- | ---: | ---: |
| Eventos por día | 720 | 43.200.000.000 |
| Tasa de ingesta sostenida | 0,008/s | 500.000/s |
| JSON crudo por día | 210 KiB | ~13 TB |
| Parquet por día | | ~1,6 TB |
| Parquet por año | | ~0,6 PB |

## B. Esquema de eventos en PySpark

`carbon_kg` y `genai_tokens` son nullable, y eso es lo que permite que las filas v1 entren con esas
dos columnas vacías en lugar de ser rechazadas. `value` es string para que una medición mal formada
sobreviva la lectura y pueda ir a Quarantine en Silver.

| Campo | Tipo en Bronze | Nullable | Presente en | Notas |
| :--- | :--- | :--- | :--- | :--- |
| `event_id` | string | no | v1, v2 | Clave del evento, y la clave de dedupe para las re-ejecuciones. |
| `timestamp` | timestamp | no | v1, v2 | ISO-8601 UTC uniforme en todos los eventos. De acá sale la partición `event_date`. |
| `org_id` | string | no | v1, v2 | Clave de join contra los maestros. |
| `resource_id` | string | no | v1, v2 | Clave de join contra `resources.csv`. |
| `service` | string | no | v1, v2 | 6 valores, consistentes con los maestros. |
| `region` | string | no | v1, v2 | 7 valores, consistentes con los maestros. |
| `metric` | string | no | v1, v2 | `requests`, `cpu_hours`, `storage_gb_hours`. |
| `value` | string | sí | v1, v2 | Se lee como texto a propósito: 41.014 llegan numéricos, 1.309 como texto, 877 nulos. Se castea en Silver. |
| `unit` | string | sí | v1, v2 | 2.075 nulos. Es una regla de calidad, no un problema de esquema. |
| `cost_usd_increment` | double | sí | v1, v2 | Acá siempre numérico, pero puede ser negativo. |
| `schema_version` | int | no | v1, v2 | `1` o `2`. Lo guardamos para poder rastrear con qué layout llegó una fila. |
| `carbon_kg` | double | sí | solo v2 | Nulo en todas las filas v1. En la fuente viene como int y como float, por eso double. |
| `genai_tokens` | long | sí | v2, `service = genai` | Nulo en v1 y en todos los eventos v2 que no son de GenAI. |

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, LongType, TimestampType,
)

USAGE_EVENT_SCHEMA = StructType([
    StructField("event_id",           StringType(),    nullable=False),
    StructField("timestamp",          TimestampType(), nullable=False),
    StructField("org_id",             StringType(),    nullable=False),
    StructField("resource_id",        StringType(),    nullable=False),
    StructField("service",            StringType(),    nullable=False),
    StructField("region",             StringType(),    nullable=False),
    StructField("metric",             StringType(),    nullable=False),
    StructField("value",              StringType(),    nullable=True),
    StructField("unit",               StringType(),    nullable=True),
    StructField("cost_usd_increment", DoubleType(),    nullable=True),
    StructField("schema_version",     IntegerType(),   nullable=False),
    StructField("carbon_kg",          DoubleType(),    nullable=True),
    StructField("genai_tokens",       LongType(),      nullable=True),
])
```

## C. Vista intradía provisoria

La versión en un párrafo es §4.4.

| Aspecto | Decisión |
| :--- | :--- |
| Qué guarda | `cost_usd` y `requests` provisorios por `org_id`, `service` y `event_date`, para el día en curso y el anterior |
| Quién la escribe | El job de streaming, en `foreachBatch`, agregando solo las filas de ese micro-batch |
| Cómo se mantiene idempotente | La clave en Cassandra incluye el `batchId` de Spark, así reprocesar un micro-batch sobreescribe su propia fila en lugar de sumar una segunda contribución. Sin counters y sin leer para escribir |
| Cómo se lee | La consulta suma las filas por batch del `event_date` pedido y reporta el número como provisorio |
| Cómo termina | La corrida diaria escribe la fila final en `org_daily_usage_by_service` y borra las filas provisorias de ese día. Un TTL un poco más largo que el SLA del batch es la red de contención si una corrida falla |
| Por qué la late data no la rompe | La vista nunca dice estar completa. Un evento tardío sube el número provisorio cuando llega, y el recálculo diario lo cierra sin importar el orden de llegada |

Usar el `batchId` en la clave cuesta filas. Con un trigger de un minuto salen hasta 1.440 batches por
día, y un día con mucho movimiento para una organización y un servicio puede acumular esa cantidad de
filas antes del cierre. Para una vista que se lee de forma interactiva y se borra todos
los días es aceptable, y es lo que compra la idempotencia sin counters. La alternativa, una sola fila
por clave más un registro aparte de los batches ya aplicados, es más piezas móviles para la misma
garantía.

## D. Detalle de MapReduce

### Entrada y splits

La entrada es Silver y no Bronze. Correr el mismo flujo directo sobre Bronze implicaría hacer la
conformación, el cast, la imputación y el dedupe dentro del mapper, sin un buen lugar donde poner las
filas rechazadas más que una salida lateral.

Silver es Parquet particionado por `event_date`, y una corrida para un rango de fechas lee solo esos
directorios. Cada split es un bloque de un archivo y se convierte en una tarea de map.

| | Hoy | Proyectado |
| :--- | ---: | ---: |
| Datos por día | ~30 KB | ~1,6 TB |
| Splits por día con bloques de 128 MB | 1 | ~12.500 |
| Claves de reduce por día, organizaciones por servicios | 480 | 300.000 |

El combiner es lo que hace viable esa segunda columna. En la escala proyectada un día tiene 12.500
tareas de map y 300.000 claves distintas: sin combiner el shuffle movería 43.200 millones de
registros. Con combiner, cada tarea de map emite como máximo la cantidad de claves distintas que vio.

### Los pasos de map y reduce

Cada medida viaja como un par, una suma y un conteo de valores presentes, para que una medición nula
no se convierta en un cero más adelante.

```text
map(record):
    key  = (record.org_id, date(record.timestamp), record.service)
    cost = record.cost_usd_increment

    # metric nombra el slot al que va el value de este evento; los otros quedan vacios
    slot = {"requests": 0.0, "cpu_hours": 0.0, "storage_gb_hours": 0.0}
    seen = {"requests": 0,   "cpu_hours": 0,   "storage_gb_hours": 0}
    if record.value is not null and record.metric in slot:
        slot[record.metric] = record.value
        seen[record.metric] = 1

    # cada medida viaja como (suma, conteo de valores presentes)
    emit(key, {
        cost:     (cost or 0.0, 1 if cost is not null else 0),
        requests: (slot.requests, seen.requests),
        cpu:      (slot.cpu_hours, seen.cpu_hours),
        storage:  (slot.storage_gb_hours, seen.storage_gb_hours),
        genai:    (record.genai_tokens or 0, 1 if record.genai_tokens is not null else 0),
        carbon:   (record.carbon_kg or 0,    1 if record.carbon_kg is not null else 0),
        events:        1,
        negative_cost: 1 if cost is not null and cost < -0.01 else 0,
        unit_imputed:  1 if record.unit_imputed else 0,
    })
```
```text
reduce(key, values):
    a = elementwise_sum(values)        # de a pares: (suma, conteo) por medida

    # una medida sin ningun valor presente informa null, nunca 0
    emit(key, {
        daily_cost_usd:   a.cost.sum     if a.cost.n     > 0 else null,
        requests:         a.requests.sum if a.requests.n > 0 else null,
        cpu_hours:        a.cpu.sum      if a.cpu.n      > 0 else null,
        storage_gb_hours: a.storage.sum  if a.storage.n  > 0 else null,
        genai_tokens:     a.genai.sum    if a.genai.n    > 0 else null,
        carbon_kg:        a.carbon.sum   if a.carbon.n   > 0 else null,
        events: a.events, negative_cost_events: a.negative_cost,
        unit_imputed_events: a.unit_imputed,
    })
```

### El mismo job con la API de DataFrames

```python
(silver_events
 .groupBy("org_id", "usage_date", "service")
 .agg(F.sum("cost_usd_increment").alias("daily_cost_usd"),
      F.sum(F.when(F.col("metric") == "requests", F.col("value_double"))).alias("requests"),
      F.sum(F.when(F.col("metric") == "cpu_hours", F.col("value_double"))).alias("cpu_hours"),
      F.sum(F.when(F.col("metric") == "storage_gb_hours", F.col("value_double"))).alias("storage_gb_hours"),
      F.sum("genai_tokens").alias("genai_tokens"),
      F.sum("carbon_kg").alias("carbon_kg"),
      F.count("*").alias("events"),
      F.sum(F.col("cost_anomaly_flag").cast("int")).alias("negative_cost_events"),
      F.sum(F.col("unit_imputed").cast("int")).alias("unit_imputed_events")))
```

El `sum` de Spark ignora los nulos y devuelve null cuando todas las entradas son nulas, con lo cual hace
gratis lo que el par suma y conteo de §7.1 hace a mano. MapReduce además escribe a HDFS entre job y
job: un mart que necesita un join con dimensiones, después una agregación y después una pasada de
anomalías son tres jobs y dos idas y vueltas a disco, mientras que Spark mantiene los datos
intermedios en memoria entre stages y hace broadcast de las dimensiones chicas en lugar de necesitar
un join del lado del map contra un cache distribuido.

## E. Fuente del diagrama de arquitectura

La figura de §6.1 se genera desde [`architecture_v1.mmd`](architecture_v1.mmd), que es la única
fuente. Para regenerarla después de editarla:

```bash
npx -y @mermaid-js/mermaid-cli -i docs/architecture_v1.mmd -o docs/architecture_v1.svg \
  -b white --no-font-embed
```

Conviene dejar las opciones de layout por defecto. Subir la tipografía y apretar el espaciado hacen
que la figura quede más alta, que es lo contrario de lo que necesita un diagrama de una página.
