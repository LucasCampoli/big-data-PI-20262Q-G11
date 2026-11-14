# Guion de demo, revisión del 16/11

Unos 10 minutos, en el orden de la jornada: ingesta, Spark, serving y analítica. Cada paso tiene su
evidencia guardada por si algo falla en vivo (consigna §8.4).

## Antes de la clase

| Cuándo | Qué | Para qué |
| :--- | :--- | :--- |
| la noche anterior | `make rerun-evidence` y `make test` | dejar la evidencia fresca y confirmar que todo pasa |
| 15 minutos antes | notebook enchufada, `make cassandra-up` | Cassandra tarda 1 a 2 minutos en responder |
| 15 minutos antes | `make reset` | arrancar la demo con el lake vacío |
| 15 minutos antes | abrir `docs/entrega-2.pdf` y `evidence/entrega-2/` | respaldo a mano |

## Recorrido

| # | Qué se muestra | Comando | Qué decir | Respaldo si falla |
| :-- | :--- | :--- | :--- | :--- |
| 1 | Arquitectura v2 | abrir el diagrama de `docs/entrega-2.md` | solo lo implementado; el speed layer queda para el final | el PDF |
| 2 | Carga batch | `make bronze` | 7 CSV con esquema explícito y chequeo de encabezado; registro con sha256 | `run_1_*.log` |
| 3 | Streaming | `make stream` | 12 micro-batches de 3.600 eventos, 0 descartados por el watermark sobre `ingest_ts`, dos checkpoints | `stream_bronze.md` |
| 4 | Re-ejecución del stream | `make stream` otra vez | el checkpoint ya tiene los 120 archivos: 0 filas | `counts_run_2.json` |
| 5 | Silver y calidad | `make silver`, después `make fixtures` | datos reales: 0 en Quarantine, 2.075 `unit` imputados, 211 costos negativos marcados; fixtures: cada regla dispara | `quarantine_samples.md` |
| 6 | Gold y plan | `make gold` | conciliación a 0,01 USD; partial aggregate = combiner, exchange = shuffle | §3.3 de `docs/entrega-2.md` |
| 7 | Serving | `make serve`, `make queries` | una partición por organización, Q2 ordena 54 filas en el cliente | `serving_q1_cqlsh.txt`, `serving_q2_cqlsh.txt` |
| 8 | Analítica | `serving_q6_cqlsh.txt` | 180 días marcados con MAD sobre 28 días; el caso de 191,90 USD contra 11,27 | §6 de `docs/entrega-2.md` |
| 9 | Idempotencia | `make run` una vez más y `make queries` | mismos conteos en todas las tablas y en Cassandra | `counts_run_1.json`, `counts_run_2.json` |

## Si falla algo

| Falla | Qué hacer |
| :--- | :--- |
| Cassandra no levanta | seguir con los pasos 2 a 6 y mostrar `serving_q*_cqlsh.txt`; el serving queda en la evidencia |
| Spark no arranca | `make check` muestra si es Java; si no se resuelve rápido, recorrer los reportes de `evidence/entrega-2/` |
| No hay conexión a internet | no hace falta: todo corre local, Cassandra incluida |
| Se corta la corrida a mitad | `make reset` y volver a correr la etapa; los checkpoints y el registro evitan duplicados |
