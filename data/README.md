# Datos

Los archivos de origen van en `landing/`. Una vez que están ahí no se tocan: Landing es inmutable
(ver D8 en `../DECISIONS.md`). El `README.md` de la raíz explica cómo conseguir el dataset y dónde
descomprimirlo.

`landing/` está en el `.gitignore`: los datos crudos no se commitean y cada uno trabaja con
su propia copia. No commitear secretos ni credenciales.

`sample/` sí se commitea: es un recorte chico de Landing con el mismo layout, para correr el notebook
sin el dataset completo. Lo genera `../scripts/make_sample.py` y no se edita a mano.
