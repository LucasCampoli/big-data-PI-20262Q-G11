# Proyecto integrador de Big Data - ITBA - Grupo 11.

Cloud Provider Analytics: ingestar y conformar datos de clientes para FinOps, Soporte y Producto.

Pipeline PySpark de Landing a Bronze, Silver y Gold en Parquet, con ingesta batch y Structured
Streaming, reglas de calidad con Quarantine, un mart FinOps diario, un mart de anomalías de costo y
serving en Cassandra.

| Documento | Qué tiene |
| :--- | :--- |
| [docs/entrega-2.md](docs/entrega-2.md) | entrega 2: qué se implementó, arquitectura v2, evidencia y backlog. Se entrega como `docs/entrega-2.pdf` |
| [docs/design.md](docs/design.md), [docs/appendix.md](docs/appendix.md) | diseño de la entrega 1, que es el contrato de la implementación (`docs/entrega-1.pdf`) |
| [DECISIONS.md](DECISIONS.md) | las 17 decisiones, con alternativas y consecuencias |
| [docs/governance.md](docs/governance.md), [docs/execution_sheet.md](docs/execution_sheet.md) | matriz de gobierno y ficha de ejecución |
| [evidence/](evidence/README.md) | perfiles de Landing, logs de corrida, conteos, muestras de Quarantine y salidas de las consultas |

## Quickstart

### Requisitos

| Herramienta | Versión probada | Para qué |
| :--- | :--- | :--- |
| Linux o WSL2 con Ubuntu | Ubuntu 24.04 | |
| Python | 3.12 | PySpark 4.2.0 y el driver de Cassandra |
| JDK | 21 (17 también sirve) | Spark. Si el `java` por defecto es más nuevo, se indica la ruta en `PIPELINE_JAVA_HOME` |
| Docker con Compose v2 | Docker Desktop 4.86, Engine 29.7, Compose 5.3 | Cassandra 5.0 local |
| make | GNU make 4 | los comandos |

Con unos 4 GB de RAM libres alcanza: Spark usa 2 GB y Cassandra 1 GB de heap.

### 1. Instalar

```bash
git clone https://github.com/LucasCampoli/big-data-PI-20262Q-G11.git
cd big-data-PI-20262Q-G11
make setup        # crea .venv, instala requirements.txt y crea config/pipeline.env
```

`make setup` copia `config/pipeline.env.example` a `config/pipeline.env` y genera ahí una sal
aleatoria para el hash de emails. Ese archivo no se commitea. Revisar `PIPELINE_JAVA_HOME`: por
defecto apunta a `/usr/lib/jvm/java-21-openjdk-amd64` (paquete `openjdk-21-jdk` de Ubuntu).

### 2. Conseguir el dataset

Descargar el dataset del desafío Cloud Provider Analytics que entrega la cátedra, descomprimirlo y
copiar el contenido de su carpeta `datalake/landing/` dentro de `data/landing/`:

```text
data/landing/customers_orgs.csv
data/landing/users.csv
data/landing/resources.csv
data/landing/support_tickets.csv
data/landing/marketing_touches.csv
data/landing/nps_surveys.csv
data/landing/billing_monthly.csv
data/landing/usage_events_stream/events_part_0000.jsonl   (120 archivos)
```

Sin el dataset, `data/sample/` trae un recorte con el mismo layout (20 organizaciones, 10 partes),
y se usa con `DATA_LANDING=data/sample make run`.

### 3. Correr

```bash
make check          # Java, Spark y Landing; falla con un mensaje si algo falta
make cassandra-up   # Cassandra 5.0 en Docker; espera a que responda CQL (1 a 2 minutos)
make run            # el pipeline completo: Bronze, Silver, Gold, anomalías y carga a Cassandra
make queries        # Q1, Q2 y Q6 por el driver y por cqlsh, salidas en evidence/entrega-2/
```

Una corrida completa tarda alrededor de un minuto. Cada corrida deja `logs/<run_id>.log` y
`logs/<run_id>.json` con la configuración, el commit y las métricas de cada etapa.

Las etapas también se corren por separado, en este orden: `make bronze`, `make stream`,
`make silver`, `make gold`, `make serve`. `make cqlsh` abre una consola CQL en el contenedor.

### 4. Probar

```bash
make test           # pruebas con Spark local, no necesitan Cassandra
make fixtures       # el pipeline sobre tests/fixtures/landing, con un lake aparte: ejercita cada regla de Quarantine
make rerun-evidence # desde cero: el pipeline dos veces seguidas, conteos y todos los reportes de evidence/entrega-2
```

`make rerun-evidence` borra el lake y el keyspace de Cassandra antes de empezar.

### 5. Limpiar y reiniciar

| Comando | Qué borra |
| :--- | :--- |
| `make reset` | `datalake/` entero: Bronze, Silver, Gold, Quarantine, checkpoints y registro de ingestados. Landing nunca se toca |
| `make reset-stream` | solo los dos checkpoints del streaming y sus salidas, para reprocesar los eventos desde Landing |
| `make reset-serving` | el keyspace de Cassandra; `make serve` lo vuelve a crear |
| `make cassandra-down` | detiene el contenedor y conserva el volumen de datos |

### Streaming continuo

Por defecto el stream procesa lo que hay en Landing y se detiene (`STREAM_TRIGGER=available_now`).
Para dejarlo corriendo con un micro-batch por minuto, como en el diseño:

```bash
STREAM_TRIGGER=processing_time make stream    # Ctrl+C lo detiene; el checkpoint queda consistente
```

### AstraDB

El mismo código carga y consulta AstraDB cambiando solo la conexión:

1. Crear una cuenta en astra.datastax.com y una base Serverless en el free tier.
2. Desde la consola de la base, en Keyspaces, crear el keyspace `cloud_analytics`. AstraDB no
   acepta `CREATE KEYSPACE` por CQL.
3. Generar un token con el rol Database Administrator y descargar el secure connect bundle. Guardar
   los dos fuera del repo.
4. Exportar las variables y correr:

```bash
export SERVING_TARGET=astra
export ASTRA_SECURE_CONNECT_BUNDLE=$HOME/secrets/secure-connect-cpa.zip
export ASTRA_TOKEN='AstraCS:...'
make serve                                      # crea las tablas de cql/01_tables.cql y carga los dos marts
PYTHONPATH=src .venv/bin/python -m pipeline.queries   # deja evidence/entrega-2/serving_q*_driver_astra.txt
```

El free tier hiberna la base después de 48 horas sin uso: antes de una demo hay que abrirla en la
consola para que se reanude.

### Solución de problemas

| Síntoma | Causa y arreglo |
| :--- | :--- |
| `Java 25 found ... pinned to JDK 17 or 21` | el `java` del PATH es otro: fijar `PIPELINE_JAVA_HOME` en `config/pipeline.env` |
| `Landing at ... is incomplete` | faltan archivos en `data/landing/`, ver el paso 2 |
| `cannot reach cassandra` | el contenedor no está arriba: `make cassandra-up` |
| `make cassandra-up` tarda o el contenedor queda `unhealthy` | después de un apagado brusco Cassandra compacta varios minutos; esperar, o `make cassandra-down cassandra-up` |
| `OperationTimedOut` en cqlsh | el nodo está ocupado; los targets ya usan `--request-timeout=120`, reintentar |
| el stream falla al arrancar después de cambiar su código | el checkpoint ya no es compatible: `make reset-stream` (D14) |
| el puerto 9042 está ocupado | otro Cassandra local; detenerlo o cambiar el puerto en `infra/docker-compose.yml` y `CASSANDRA_PORT` |
| la máquina se pone lenta | excluir `datalake/`, `logs/` y `.venv/` del watcher del editor, `make cassandra-down` cuando no se usa, `SPARK_MASTER=local[2]` |

## Estructura

```text
Makefile         los comandos
src/pipeline/    código: config, sesión de Spark, esquemas, Bronze, Silver, Gold, anomalías, serving, evidencia
tests/           pruebas y la Landing de fixtures (tests/fixtures/landing)
cql/             keyspace, tablas y consultas de Cassandra
infra/           docker-compose de Cassandra
config/          pipeline.env.example, sin secretos
data/            muestra commiteada en data/sample/; los archivos crudos van en data/landing/
docs/            entregas, diseño, gobierno, ficha de ejecución, diagramas y la consigna
evidence/        perfiles de Landing (entrega 1) y evidencia de corrida (entrega-2/)
notebooks/       exploración de Landing con PySpark (entrega 1)
scripts/         armado del HTML imprimible, la muestra de datos y la corrida del notebook
datalake/, logs/ salidas de las corridas, fuera de git
```

## Documento imprimible

`python scripts/build_entrega_html.py . 2` arma `docs/entrega-2.html` con el documento, la matriz
de gobierno, la ficha, el índice de decisiones y el diagrama embebido (con `1`, el de la entrega 1).
Se imprime desde el navegador en A4, con fondos activados y sin encabezado ni pie.

## Notebook de exploración

`bash scripts/run_exploration.sh` corre `notebooks/01_landing_exploration.ipynb` sin interfaz y deja
el log en `evidence/landing_exploration_run.log`. Los perfiles que genera están en `evidence/`.

## Convenciones

El código, los nombres de tablas, columnas y archivos, el notebook, los logs y los mensajes de
commit van en inglés. La documentación que lee la cátedra va en español.

`main` tiene lo que se entrega, y es lo que se evalúa a la hora del corte. Se trabaja en ramas
cortas con nombre `<área>/<tema>`, por ejemplo `silver/quality-rules` o `serving/keyspace`, y se
mergean a `main` cuando la pieza está terminada. Las áreas siguen los roles de
[docs/design.md](docs/design.md) §8.4: `ingestion`, `silver`, `marts`, `serving`, `docs`.

| Qué | Convención | Ejemplo |
| --- | --- | --- |
| Módulos y funciones de Python | `snake_case` | `quality_rules.py` |
| Notebooks | `NN_topic.ipynb`, numerados en orden de ejecución | `01_landing_exploration.ipynb` |
| Rutas del lake | `datalake/<zona>/<entidad>/<partición>=<valor>/` | `datalake/silver/usage_events/event_date=2025-08-01/` |
| Columnas | `snake_case` | `cost_usd_increment` |
| Columnas técnicas | conjunto fijo, mismos nombres en todas las zonas | `ingest_ts`, `ingest_date`, `source_file`, `processed_ts`, `run_id` |
| Flags de reparación y calidad | booleanos, con el nombre de lo que pasó | `unit_imputed`, `fx_overridden`, `cost_anomaly_flag`, `dq_status` |
| Marts de Gold | `<grano>_<sujeto>_by_<dimensión>` | `org_daily_usage_by_service` |
| Archivos de evidencia | `<fuente>_<artefacto>.<ext>`, se regeneran, no se editan a mano | `landing_profile.md` |
