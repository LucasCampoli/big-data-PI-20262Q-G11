# Checklist de la entrega 1

Verificado el 2026-10-05 contra la consigna. La puerta de entrada a todo lo que sigue es
[`../docs/design.md`](../docs/design.md).

## Checklist §9.1

| # | Ítem | Dónde está | Estado |
| :-- | :--- | :--- | :--- |
| 1 | Documento de diseño disponible en el canal de entrega | `docs/entrega-1.pdf`, generado desde `docs/design.md` | Listo, la subida en sí es manual |
| 2 | Repositorio accesible y versionado | este repo, tag `entrega-1` | Hecho |
| 3 | Interpretación del caso y objetivos medibles | §1, con O1 a O3 en §1.3 | Hecho |
| 4 | Análisis 5V | §2, con la proyección en el apéndice A | Hecho |
| 5 | Inventario y perfil de fuentes | §3, medido en `landing_profile.md` | Hecho |
| 6 | Arquitectura v1 y patrón justificado | §6.1 y `architecture_v1.svg`, el patrón en §4 | Hecho |
| 7 | Diseño Landing/Bronze/Silver/Gold | §5, más Quarantine como quinta zona | Hecho |
| 8 | Flujos batch y streaming | §6.2 y §6.3 | Hecho |
| 9 | Lógica MapReduce o equivalente | §7, pseudocódigo en §7.1 y completo en el apéndice D | Hecho |
| 10 | Matriz requisito-componente | §6.4 | Hecho |
| 11 | Supuestos, riesgos, mitigaciones y estimación de esfuerzo | §8.1, §8.2, §3.1, §8.4 | Hecho |
| 12 | Evidencia mínima de lectura o exploración de datos | `01_landing_exploration.ipynb` con sus salidas, los perfiles y el log de corrida en `evidence/`, muestra en `data/sample/` | Hecho |

## Criterios de aceptación §5.4

| # | Criterio | Dónde está | Estado |
| :-- | :--- | :--- | :--- |
| 1 | Problema, usuarios y criterios de éxito sin ambigüedad | §1.1 usuarios, §1.2 preguntas, §1.3 tres objetivos numerados con umbral | Hecho |
| 2 | La arquitectura responde a los requisitos y distingue batch de streaming | §4.3 qué corre dónde, §6.2 y §6.3 como flujos separados, §6.4 mapea cada requisito | Hecho |
| 3 | Zonas, formatos y particiones coherentes con los datos provistos | §5, con la elección de partición argumentada sobre conteos de archivos medidos en §5.1 | Hecho |
| 4 | El flujo MapReduce muestra cómo se resolvería el batch | §7 calcula `org_daily_usage_by_service`, el mart obligatorio | Hecho |
| 5 | Supuestos y riesgos realistas, con mitigaciones | §8.1 diez supuestos con su consecuencia, §8.2 seis riesgos de proyecto, §3.1 cinco riesgos de datos | Hecho |
| 6 | El repo y la documentación permiten seguir implementando | README con quickstart y convenciones, `DECISIONS.md` con 12 registros, esfuerzo por workstream en §8.4 | Hecho |

## Puntos abiertos

Nada del alcance obligatorio quedó afuera. Hay dos cosas sin cerrar, a propósito.

| Punto | Por qué |
| :--- | :--- |
| D11, el modelo de tablas en Cassandra | Necesita los conteos de filas de los marts, que existen recién cuando corre Gold. Se decide antes de escribir el keyspace en la entrega 2 |
| D12, el método de anomalías | Necesita la distribución de la serie diaria agregada, no de los incrementos crudos. Se decide junto con `cost_anomaly_mart` |

El conjunto de reglas de calidad todavía no tiene código atrás, y las muestras de quarantine que pide
la entrega 2 salen de los fixtures planificados en D10. Eso es alcance de la entrega 2 y no un hueco
de esta.
