# Gobierno del dato

Quién es dueño de cada activo, qué tan sensible es, qué reglas lo protegen, cómo se rastrea, quién lo
puede leer y cómo se vigila. Los dueños son los roles de `docs/design.md` §8.4. El esquema de cada
tabla, con una nota por columna, está en `docs/data_dictionary.md`, generado desde el lake.

## Clasificación

| Nivel | Qué entra | Ejemplos |
| :--- | :--- | :--- |
| Personal | datos que identifican a una persona | `users.email` |
| Confidencial | datos comerciales de un cliente | facturación, nombre de la organización, tickets, uso y costo por organización |
| Interna | agregados y metadatos operativos | logs de corrida, registro de ingestados, checkpoints |

## Matriz de activos

| Activo | Dueño | Sensibilidad | Acceso | Retención |
| :--- | :--- | :--- | :--- | :--- |
| Landing, 7 CSV y 120 JSONL | Ingesta | Personal (`users.csv`) | solo los jobs de ingesta y la exploración de solo lectura | histórico completo, inmutable (D8) |
| `bronze/usage_events` | Ingesta | Confidencial | jobs de Silver | 90 días sobre `ingest_date` |
| `bronze/<maestro>`, 7 tablas | Ingesta | Personal (`users`), Confidencial (resto) | jobs de Silver | 90 días sobre `ingest_date` (`month` en facturación) |
| `silver/usage_events` | Calidad y Silver | Confidencial | jobs de Gold, notebooks del equipo | histórico completo |
| `silver/users` | Calidad y Silver | Confidencial: sin email, con `email_hash` y `email_domain` | jobs de Gold | histórico completo |
| `silver/billing_monthly`, `silver/support_tickets` y demás maestros | Calidad y Silver | Confidencial | jobs de Gold | histórico completo |
| `gold/org_daily_usage_by_service`, `gold/cost_anomaly_mart` | Marts | Confidencial | carga a Cassandra, BI | 13 meses sobre `usage_date` |
| `gold/top_services_14d_by_org` | Marts | Confidencial | carga a Cassandra | 13 meses sobre `as_of_date` |
| `gold/tickets_by_org_date`, `gold/tickets_daily_by_org` | Marts | Confidencial | carga a Cassandra (la diaria), BI | 13 meses sobre `ticket_date` |
| `gold/revenue_by_org_month` | Marts | Confidencial | carga a Cassandra, FinOps | histórico completo, como `silver/billing_monthly` |
| `gold/genai_tokens_by_org_date` | Marts | Confidencial | carga a Cassandra, Producto | 13 meses sobre `usage_date` |
| `quarantine/*`, 4 tablas | Calidad y Silver | la del origen: `raw_record` puede traer un email | ingenieros del equipo, como cola de revisión | 180 días las de append; las de reglas se reconstruyen |
| Cassandra `cloud_analytics`, 6 tablas | Serving | Confidencial, sin datos personales | `cpa_loader` escribe, `cpa_reader` lee (D19) | la del mart de Gold |
| AstraDB `cpa`, 6 tablas | Serving | Confidencial, sin datos personales | por token de la consola | la del mart de Gold |
| `_registry`, `_checkpoints`, `logs/` | Ingesta | Interna | ingenieros del equipo | lo que dure la query; logs 90 días |

| Activo | Reglas de calidad | Métricas | Linaje | Monitoreo |
| :--- | :--- | :--- | :--- | :--- |
| Landing | ninguna, se observa (design §5) | archivos y filas por fuente | nombre de archivo | `make check` falla si falta una fuente e informa archivos ajenos |
| Bronze | parsea con el esquema declarado, encabezado CSV igual al esquema | filas, filas a Quarantine, claves repetidas, progreso por micro-batch | `source_file`, `ingest_ts`, `ingest_date`, sha256 en el registro | `make health`: filas descartadas por el watermark |
| Silver | D6 y D7: 8 reglas de rechazo de eventos, 1 de tickets, 8 flags de reparación | O3 en filas y en costo, `unit` con `value` al 100% | lo de Bronze, más `processed_ts` y `run_id` | `make health`: O3 y crecimiento de Quarantine |
| Gold, mart diario | claves no nulas, filas igual al grano, costo a 0,01 USD de Silver por organización y día | resultado de la conciliación | `run_id`, `computed_ts`, `events` por fila | la corrida no publica si no concilia; `make health` |
| Gold, marts de Q2 a Q5 | claves no nulas, una fila por clave, totales iguales a su fuente (D18) | resultado de cada control | `run_id`, `computed_ts` | la corrida no publica si no concilia; `make health` |
| Quarantine | es el control | filas por regla y por tabla | `raw_record`, `source_file`, `rule_name`, `run_id` | `make health`: O3 y crecimiento |
| Cassandra y AstraDB | filas escritas iguales a Gold, por tabla | filas por tabla | `run_id`, `computed_ts` | la carga falla si no coincide; `make health` |

La evidencia de cada fila está en `evidence/final/README.md`.

## Linaje por consulta

| Consulta | Tabla de Cassandra | Mart de Gold | Silver | Landing |
| :--- | :--- | :--- | :--- | :--- |
| Q1 | `org_daily_usage_by_service` | `org_daily_usage_by_service` | `usage_events` + `customers_orgs` + `resources` | `usage_events_stream/*.jsonl` y dos CSV |
| Q2 | `top_services_14d_by_org` | `top_services_14d_by_org`, desde el mart diario | igual que Q1 | igual que Q1 |
| Q3 | `tickets_daily_by_org` | `tickets_daily_by_org` | `support_tickets` | `support_tickets.csv` |
| Q4 | `revenue_by_org_month` | `revenue_by_org_month` | `billing_monthly` | `billing_monthly.csv` |
| Q5 | `genai_tokens_by_org_date` | `genai_tokens_by_org_date` | `usage_events` (servicio genai) | `usage_events_stream/*.jsonl` |
| Q6 | `cost_anomalies_by_org` | `cost_anomaly_mart`, desde el mart diario | igual que Q1 | igual que Q1 |

Un número servido se rastrea hasta su archivo de origen con las columnas técnicas:

1. La fila de Cassandra trae `run_id` y `computed_ts`. Con el `run_id` se abre `logs/<run_id>.json`,
   que tiene la configuración y las métricas de cada etapa, incluidos los controles de cada mart.
2. La misma clave en el mart de Gold dice sobre cuántas filas de Silver se apoya (`events`,
   `tickets`, `invoices`).
3. En Silver, filtrando por esa clave, aparecen las filas con su `source_file` e `ingest_ts`.
4. El registro de ingestados y el checkpoint del stream dicen cuándo entró cada archivo de Landing, y
   el sha256 del registro prueba que el CSV no cambió desde entonces.

## Datos personales

`users.email` es el único dato personal del dataset. Hasta Bronze se guarda tal cual, porque Bronze
es la copia fiel de la fuente. Silver lo reemplaza por `email_hash`, un sha256 de una sal más el
email normalizado, y `email_domain` (D17). La sal (`PII_HASH_SALT`) la genera `make setup` en
`config/pipeline.env`, que no se commitea. Sin sal la corrida se detiene en `make check` y en
Silver, antes de escribir `users`. Ningún mart ni tabla de Cassandra tiene datos personales.

## Acceso y credenciales

| Dónde | Quién | Cómo |
| :--- | :--- | :--- |
| Cassandra local | `cpa_loader`: `SELECT` y `MODIFY`; `cpa_reader`: `SELECT`; el superusuario solo en `make cassandra-init` | `PasswordAuthenticator` y `CassandraAuthorizer` (D19) |
| AstraDB | el token de la consola | mTLS por el secure connect bundle |
| Lake y logs | un usuario en la máquina local, separación por carpeta y por escritor (D15) | permisos del sistema de archivos |

- Nada secreto vive en git. `config/pipeline.env`, `.env`, `*.pem` y los bundles `secure-connect-*.zip`
  están en `.gitignore`, y una prueba de `make test` falla si un archivo trackeado contiene un token,
  una clave privada o un valor de `config/pipeline.env`.
- Las contraseñas de Cassandra, la sal y el token de AstraDB se leen solo de variables de entorno.
  El log de cada corrida muestra la configuración con los secretos reemplazados por `<set>` o `<unset>`.
- La evidencia copiada de los logs reemplaza la ruta local del repo por `<repo>` y el host de AstraDB
  por `<astra-host>`.

## Monitoreo

`make health` lee los resúmenes de corrida y devuelve OK, WARN o ALERT por control, con la historia
de las últimas 10 corridas; sale con código 1 si hay una ALERT (D20). Los controles cubren etapas
fallidas, frescura de Gold, filas descartadas por el watermark, O3, conciliación de Gold y de los
marts, filas de serving contra Gold y crecimiento de Quarantine.

## Lo que queda fuera del MVP

| Control | Hoy | En producción |
| :--- | :--- | :--- |
| Acceso al lake | un usuario local, separación por carpeta | un rol por job con permisos por prefijo en el object storage |
| AstraDB | un token con rol Database Administrator | un token de solo lectura para las consultas |
| Retención | definida por activo, no automatizada | un job que borre particiones vencidas de Bronze y Quarantine |
| Alertas | `make health` con código de salida | el mismo control enviado a un canal del equipo desde el scheduler |
| Catálogo | `docs/data_dictionary.md` generado desde el lake | catálogo con versionado de esquemas |
