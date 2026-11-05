# Datos

Los archivos de origen van en `landing/`. Una vez que están ahí no se tocan: Landing es inmutable
(ver D8 en `../DECISIONS.md`). El `README.md` de la raíz explica cómo conseguir el dataset y dónde
descomprimirlo.

`landing/` está en el `.gitignore`: los datos crudos no se commitean y cada uno trabaja con
su propia copia. No commitear secretos ni credenciales.

`sample/` sí se commitea: es un recorte chico de Landing con el mismo layout, para correr el notebook
o el pipeline sin el dataset completo (`DATA_LANDING=data/sample make run`). Lo genera
`../scripts/make_sample.py` y no se edita a mano.

Los datos sintéticos que ejercitan Quarantine no están acá sino en `../tests/fixtures/landing/`, y
nunca se copian a `landing/` (D10).
