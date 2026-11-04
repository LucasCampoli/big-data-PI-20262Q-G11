-- Keyspace for the local single-node Cassandra (infra/docker-compose.yml, datacenter dc1).
-- Same strategy as production, where it would be {'dc1': 3} and reads and writes at LOCAL_QUORUM
-- (D11). On AstraDB the keyspace is created from the console instead: CREATE KEYSPACE is not
-- available there.
CREATE KEYSPACE IF NOT EXISTS cloud_analytics
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1};
