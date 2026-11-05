# Infraestructura

`docker-compose.yml` levanta un nodo de Cassandra 5.0 para el serving local, en el datacenter `dc1`
y con 1 GB de heap. Se maneja desde la raíz del repo:

| Comando | Qué hace |
| :--- | :--- |
| `make cassandra-up` | levanta el contenedor `cpa-cassandra` y espera a que responda CQL |
| `make cqlsh` | consola CQL dentro del contenedor |
| `make reset-serving` | borra el keyspace `cloud_analytics` |
| `make cassandra-down` | detiene el contenedor y conserva el volumen `cassandra-data` |

La carpeta `../cql` se monta en `/cql` dentro del contenedor, así cqlsh corre los mismos archivos que
usa el cargador. El puerto 9042 queda publicado solo para la máquina local. AstraDB no necesita nada
de esta carpeta: se configura por variables de entorno (README de la raíz).
