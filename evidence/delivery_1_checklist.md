# Checklist de la entrega 1

Verificado el 2026-10-03 contra la consigna. La puerta de entrada a todo lo que sigue es
[`../docs/design.md`](../docs/design.md).

## Checklist §9.1

| # | Ítem | Dónde está | Estado |
| :-- | :--- | :--- | :--- |
| 1 | Documento de diseño disponible en el canal de entrega | `docs/design.md`, imprimible desde `docs/entrega-1.html` | Listo, la subida en sí es manual |
| 2 | Repositorio accesible y versionado | este repo, tag `entrega-1` | Taggeado local, falta el push por permisos |
| 3 | Interpretación del caso y objetivos medibles | §1, con O1 a O3 en §1.3 | Hecho |
| 4 | Análisis 5V | §2, con la proyección en el apéndice A | Hecho |
| 5 | Inventario y perfil de fuentes | §3, medido en `landing_profile.md` | Hecho |
| 6 | Arquitectura v1 y patrón justificado | §6.1 y `architecture_v1.svg`, el patrón en §4 | Hecho |
| 7 | Diseño Landing/Bronze/Silver/Gold | §5, más Quarantine como quinta zona | Hecho |
| 8 | Flujos batch y streaming | §6.2 y §6.3 | Hecho |
| 9 | Lógica MapReduce o equivalente | §7, pseudocódigo en §7.1 y completo en el apéndice D | Hecho |
| 10 | Matriz requisito-componente | §6.4 | Hecho |
| 11 | Supuestos, riesgos, mitigaciones y estimación de esfuerzo | §8.1, §8.2, §3.1, §8.4 | Hecho |
| 12 | Evidencia mínima de lectura o exploración de datos | `01_landing_exploration.ipynb` con sus salidas, tres archivos en `evidence/` | Hecho |

## Criterios de aceptación §5.4

| # | Criterio | Dónde está | Estado |
| :-- | :--- | :--- | :--- |
| 1 | Problema, usuarios y criterios de éxito sin ambigüedad | §1.1 usuarios, §1.2 preguntas, §1.3 tres objetivos numerados con umbral | Hecho |
| 2 | La arquitectura responde a los requisitos y distingue batch de streaming | §4.3 qué corre dónde, §6.2 y §6.3 como flujos separados, §6.4 mapea cada requisito | Hecho |
| 3 | Zonas, formatos y particiones coherentes con los datos provistos | §5, con la elección de partición argumentada sobre conteos de archivos medidos en §5.1 | Hecho |
| 4 | El flujo MapReduce muestra cómo se resolvería el batch | §7 calcula `org_daily_usage_by_service`, el mart obligatorio | Hecho |
| 5 | Supuestos y riesgos realistas, con mitigaciones | §8.1 diez supuestos con su consecuencia, §8.2 seis riesgos de proyecto, §3.1 cinco riesgos de datos | Hecho |
| 6 | El repo y la documentación permiten seguir implementando | README con quickstart y convenciones, `DECISIONS.md` con 12 registros, backlog en §8.5 | Hecho |

## Puntos abiertos

Nada del alcance obligatorio quedó afuera. Hay cuatro cosas sin cerrar, a propósito.

| Punto | Por qué |
| :--- | :--- |
| D11, el modelo de tablas en Cassandra | Necesita los conteos de filas de los marts, que existen recién cuando corre Gold. Se decide antes de escribir el keyspace en la entrega 2 |
| D12, el método de anomalías | Necesita la distribución de la serie diaria agregada, no de los incrementos crudos. Se decide junto con `cost_anomaly_mart` |
| El PDF no está commiteado | La fuente es `docs/entrega-1.html`. Se abre en el navegador y se imprime a PDF, así el archivo se genera en lugar de quedar viejo en git |
| El tag es local | Todavía no tenemos permisos de push, y `entrega-1` existe solo en el clon local |

El conjunto de reglas de calidad todavía no tiene código atrás, y las muestras de quarantine que pide
la entrega 2 salen de los fixtures planificados en §5.3. Eso es alcance de la entrega 2 y no un hueco
de esta.
