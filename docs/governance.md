# Gobierno preliminar

Controles iniciales de la entrega 2: quién es dueño de cada activo, qué tan sensible es, qué reglas
lo protegen, cómo se rastrea y quién lo puede leer. Cada fila separa lo que el pipeline ya hace de
lo que queda para el MVP final. Los dueños son los roles de `docs/design.md` §8.4.

## Clasificación

| Nivel | Qué entra | Ejemplos |
| :--- | :--- | :--- |
| Personal | datos que identifican a una persona | `users.email` |
| Confidencial | datos comerciales de un cliente | facturación, nombre de la organización, tickets, uso y costo por organización |
| Interna | agregados y metadatos operativos | marts sin datos personales, logs de corrida, registro de ingestados |

## Matriz de activos

| Activo | Dueño | Sensibilidad | Acceso | Retención |
| :--- | :--- | :--- | :--- | :--- |
| Landing, 7 CSV y 120 JSONL | Ingesta | Personal (`users.csv`) | solo los jobs de ingesta y la exploración de solo lectura | histórico completo, inmutable (D8) |
| `bronze/usage_events` | Ingesta | Confidencial | jobs de Silver | 90 días sobre `ingest_date` |
| `bronze/<maestro>`, 7 tablas | Ingesta | Personal (`users`), Confidencial (resto) | jobs de Silver | 90 días sobre `ingest_date` |
| `silver/usage_events` | Calidad y Silver | Confidencial | jobs de Gold, notebooks del equipo | histórico completo |
| `silver/users` | Calidad y Silver | Confidencial: sin email, con `email_hash` y `email_domain` | jobs de Gold | histórico completo |
| `silver/billing_monthly` y demás maestros | Calidad y Silver | Confidencial | jobs de Gold | histórico completo |
| `gold/org_daily_usage_by_service` | Marts | Confidencial | carga a Cassandra, BI | 13 meses sobre `usage_date` |
| `gold/cost_anomaly_mart` | Marts | Confidencial | carga a Cassandra, BI | 13 meses sobre `usage_date` |
| `quarantine/*`, 4 tablas | Calidad y Silver | la del origen: `raw_record` puede traer un email | ingenieros del equipo, como cola de revisión | 180 días las de append; las de reglas se reconstruyen |
| Cassandra `cloud_analytics`, 2 tablas | Serving | Confidencial, sin datos personales | FinOps y Producto, solo lectura | la del mart de Gold |
| `_registry`, `_checkpoints`, `logs/` | Ingesta | Interna | ingenieros del equipo | lo que dure la query; logs 90 días |

| Activo | Reglas de calidad | Métricas | Linaje | Monitoreo | Evidencia |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Landing | ninguna, se observa (design §5) | archivos y filas por fuente | nombre de archivo | `make check` falla si falta una fuente | `landing_profile.md` |
| Bronze | parsea con el esquema declarado, encabezado CSV igual al esquema | filas, filas a Quarantine, claves repetidas, progreso por micro-batch | `source_file`, `ingest_ts`, `ingest_date`, sha256 en el registro | resumen de corrida, `numRowsDroppedByWatermark` | `stream_bronze.md`, `run_*_stream_progress.jsonl` |
| Silver | D6 y D7: 8 reglas de rechazo de eventos, 1 de tickets, 8 flags de reparación | O3 en filas y en costo, `unit` con `value` al 100% | lo de Bronze, más `processed_ts` y `run_id` | warning si Quarantine supera el 1% (O3) | `silver_quality.md` |
| Gold | claves no nulas, filas igual al grano, costo a 0,01 USD de Silver por organización y día | resultado de la conciliación | `run_id`, `computed_ts`, `events` por fila | la corrida falla si no concilia y no publica | `gold_marts.md`, `gold_reconciliation.json` |
| Quarantine | es el control | filas por regla y por tabla | `raw_record`, `source_file`, `rule_name`, `run_id` | tamaño contra O3 | `quarantine_samples.md` |
| Cassandra | filas y costo por organización iguales a Gold | filas por tabla | `run_id`, `computed_ts` | la carga falla si no coincide | `serving_q*.txt`, `idempotency.md` |

## Linaje de punta a punta

Un número servido se rastrea hasta su archivo de origen con las columnas técnicas:

1. Una fila de Cassandra trae `run_id` y `computed_ts`. Con el `run_id` se abre `logs/<run_id>.json`,
   que tiene la configuración, el commit de git y las métricas de cada etapa de esa corrida.
2. La misma clave (`org_id`, `usage_date`, `service`) en `gold/org_daily_usage_by_service` dice sobre
   cuántos eventos se apoya (`events`), cuántos son correcciones negativas y cuántos tienen `unit`
   imputado.
3. En `silver/usage_events`, filtrando por `org_id`, `event_date` y `service`, aparecen esos eventos
   con su `source_file` e `ingest_ts`.
4. El registro de ingestados y el checkpoint del stream dicen cuándo entró cada archivo de Landing, y
   el sha256 del registro prueba que el archivo de los maestros no cambió desde entonces.

## Datos personales

`users.email` es el único dato personal del dataset. Hasta Bronze se guarda tal cual, porque Bronze
es la copia fiel de la fuente. Silver lo reemplaza por `email_hash`, un sha256 de una sal más el
email normalizado, y `email_domain`. El hash sirve para contar y deduplicar usuarios sin exponer la
dirección. La sal (`PII_HASH_SALT`) la genera `make setup` en `config/pipeline.env`, que no se
commitea. Sin sal la corrida lo avisa en el log. Ningún mart ni tabla de Cassandra tiene datos
personales.

## Credenciales y configuración

- Nada secreto vive en git. `config/pipeline.env`, `.env`, `*.pem` y los bundles `secure-connect-*.zip`
  están en `.gitignore`.
- El token y el bundle de AstraDB se leen solo de variables de entorno. El log de cada corrida
  muestra la configuración con los secretos reemplazados por `<set>` o `<unset>`.
- La evidencia copiada de los logs reemplaza la ruta local del repo por `<repo>`.

## Lo implementado y lo pendiente

| Control | Hoy | Para el MVP final |
| :--- | :--- | :--- |
| Acceso por zona | un solo usuario en la máquina local, separación por carpeta y por escritor (D15) | un rol por job con permisos por prefijo en el object storage |
| Acceso a Cassandra | contenedor local sin autenticación, solo en `127.0.0.1` | `PasswordAuthenticator`, un rol de carga con `MODIFY` y uno de lectura con `SELECT` sobre el keyspace |
| Retención | definida por zona, no automatizada | un job que borre particiones de Bronze y Quarantine vencidas por fecha de partición |
| Monitoreo | resumen JSON por corrida, warnings de O3, compuertas que cortan la corrida | alertas sobre el resumen y un tablero de tamaño de Quarantine |
| Catálogo | diccionario generado desde los Parquet (`data_dictionary.md`) | descripciones de negocio por columna, versionado de esquemas |
