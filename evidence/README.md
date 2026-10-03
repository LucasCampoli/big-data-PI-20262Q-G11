# Evidencia

Artefactos generados que respaldan lo que afirma `docs/design.md`. No se editan a mano: se regeneran
corriendo el notebook que los produce.

| Archivo | Contenido |
| :--- | :--- |
| `landing_profile.md` | Perfil completo de Landing: grano y conteo de filas por fuente, nulos, versiones de esquema de los eventos, la inconsistencia de tipo en `value`, la distribución de costos, la medición de late data, el particionado de Bronze y Silver, y la base de cada regla de calidad. |
| `landing_source_profile.csv` | Una fila por fuente: formato, archivos, filas, columnas, clave de grano, claves duplicadas, rango de fechas. |
| `landing_column_profile.csv` | Una fila por columna: cantidad de nulos y proporción de nulos. |
| `delivery_1_checklist.md` | El checklist §9.1 y los criterios de aceptación §5.4, con dónde está cada ítem. |

Los tres perfiles salen de `notebooks/01_landing_exploration.ipynb`. Están en inglés porque son
salida del notebook, que se mantiene en inglés junto con el código; el checklist, en cambio, lo
escribimos nosotros y va en español como el resto de la documentación.

Las capturas y los logs de corrida de las entregas también van acá.
