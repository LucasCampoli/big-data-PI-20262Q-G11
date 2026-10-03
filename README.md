# big-data-PI-20262Q-G11

Proyecto integrador de Big Data, grupo 11. Cloud Provider Analytics: ingestar y conformar datos de
clientes para FinOps, Soporte y Producto.

El repo está todavía en etapa de diseño. La implementación viene después.

Documento de diseño: [docs/design.md](docs/design.md). Es la puerta de entrada: problema y objetivos,
las 5V, el perfil de las fuentes, el patrón arquitectónico, el Data Lake, la arquitectura v1, el
flujo batch expresado como MapReduce y el plan. El detalle de apoyo está en
[docs/appendix.md](docs/appendix.md).

Para generar el PDF, armamos el HTML imprimible y lo imprimimos desde el navegador en A4:

```bash
python scripts/build_entrega_html.py .   # escribe docs/entrega-1.html
```

Ese archivo junta el diseño, el apéndice y las decisiones en un solo documento, con el diagrama
embebido. Está en el `.gitignore` a propósito, así se regenera siempre desde el Markdown en lugar de
quedar viejo dentro del repo.

Evidencia de datos: [evidence/landing_profile.md](evidence/landing_profile.md), producido por
[notebooks/01_landing_exploration.ipynb](notebooks/01_landing_exploration.ipynb).

Decisiones, con alternativas y trade-offs: [DECISIONS.md](DECISIONS.md).

## Estructura

```text
README.md
DECISIONS.md
requirements.txt
docs/          diseño, apéndice y la consigna
data/          datos de muestra; los archivos crudos van en data/landing/
src/           código de procesamiento (entrega 2)
notebooks/     exploración de Landing con PySpark
tests/         pruebas y fixtures de quarantine (entrega 2)
config/        paths.env.example, copiar y ajustar local, sin secretos
infra/         notas de runtime (más adelante)
evidence/      perfiles generados, el checklist de entrega, logs
scripts/       armado del HTML imprimible
```

## Cómo conseguir el dataset

Los archivos de Landing no se commitean, de modo que cada uno se arma su copia. Hay que descargar el
dataset del desafío Cloud Provider Analytics que entrega la cátedra, descomprimirlo y copiar el
contenido de su carpeta `datalake/landing/` dentro de `data/landing/`, de manera que quede así:

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

Si el zip se bajó desde Windows, conviene borrar los archivos `*:Zone.Identifier` que suele traer.

`data/landing/` está en el `.gitignore` y a Landing no se la edita nunca, por lo que rehacer la copia es
siempre seguro. Si los archivos están en otra ruta, apuntar `DATA_LANDING` ahí.

## Cómo correr el notebook de exploración

Con el dataset en su lugar:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/jupyter lab notebooks/01_landing_exploration.ipynb
```

PySpark necesita un JDK 17 o 21 en el PATH. Si el JDK por defecto es más nuevo, hay que setear
`JAVA_HOME`, y `DATA_LANDING` si los archivos de Landing no están en `data/landing/`.

Correr todas las celdas reescribe los tres archivos de `evidence/`, y los números que están
commiteados se pueden reproducir desde los datos de Landing.

Copiar `config/paths.env.example` si hace falta sobreescribir una ruta local. No commitear `.env` ni
credenciales.

## Convenciones

El código, los nombres de tablas, columnas y archivos, el notebook y los mensajes de commit van en
inglés. La documentación que lee la cátedra va en español.

Ramas. `main` tiene lo que se entrega, y es lo que se evalúa a la hora del corte. Se trabaja en ramas
cortas con nombre `<área>/<tema>`, por ejemplo `silver/quality-rules` o `serving/keyspace`, y se
mergean a `main` cuando la pieza está terminada. Las áreas siguen los roles de
[docs/design.md](docs/design.md) §8.4: `ingestion`, `silver`, `marts`, `serving`, `docs`.

Commits. Asunto corto en imperativo y en inglés, de menos de 60 caracteres, que diga qué hace el
commit: "Add the streaming job", no "added streaming" ni "changes". El cuerpo solo cuando el motivo
no se ve en el diff, y breve. Los co-autores van con trailers `Co-authored-by:` cuando el trabajo fue
compartido. Un cambio lógico por commit.

Nombres.

| Qué | Convención | Ejemplo |
| :--- | :--- | :--- |
| Módulos y funciones de Python | `snake_case` | `quality_rules.py` |
| Notebooks | `NN_topic.ipynb`, numerados en orden de ejecución | `01_landing_exploration.ipynb` |
| Rutas del lake | `datalake/<zona>/<entidad>/<partición>=<valor>/` | `datalake/silver/usage_events/event_date=2025-08-01/` |
| Columnas | `snake_case` | `cost_usd_increment` |
| Columnas técnicas | conjunto fijo, mismos nombres en todas las zonas | `ingest_ts`, `ingest_date`, `source_file`, `processed_ts`, `run_id` |
| Flags de reparación y calidad | booleanos, con el nombre de lo que pasó | `unit_imputed`, `fx_overridden`, `cost_anomaly_flag`, `dq_status` |
| Marts de Gold | `<grano>_<sujeto>_by_<dimensión>` | `org_daily_usage_by_service` |
| Archivos de evidencia | `<fuente>_<artefacto>.<ext>`, se regeneran, no se editan a mano | `landing_profile.md` |
