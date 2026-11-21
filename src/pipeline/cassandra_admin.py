"""Local Cassandra setup as superuser: keyspace, tables, and the loader and reader roles (D19).

python -m pipeline.cassandra_admin      (make cassandra-init)

Run after make cassandra-up, and again after a change in cql/01_tables.cql. It is idempotent:
roles are created if missing and their passwords set again from config, grants are repeated.

The container starts with the stock superuser cassandra/cassandra. The first run logs in with that
password and replaces it with CASSANDRA_ADMIN_PASSWORD; later runs log in with the new one.

| role        | permissions on the keyspace | used by                                   |
| cpa_loader  | SELECT, MODIFY              | make serve: upserts and the row check     |
| cpa_reader  | SELECT                      | make queries, cqlsh, the evidence counts  |
"""
import logging
import re
import sys

from .config import Config
from .errors import PipelineError
from .serving import CQL_DIR, connect, connection_settings, cql_statements

log = logging.getLogger("pipeline.cassandra_admin")

STOCK_PASSWORD = "cassandra"
# CQL takes role passwords as literals, not bind markers; anything outside this set is refused
# instead of being quoted into a statement.
SAFE_SECRET = re.compile(r"^[A-Za-z0-9_.\-]{12,}$")


def admin_session(cfg: Config):
    from cassandra.cluster import NoHostAvailable

    settings = connection_settings(cfg, "admin")
    try:
        return connect(settings)
    except NoHostAvailable as exc:
        if "AuthenticationFailed" not in str(exc) or cfg.cassandra_admin_username != "cassandra":
            raise
    # First run on a fresh volume: the superuser still has the stock password.
    cluster, session = connect({**settings, "password": STOCK_PASSWORD})
    session.execute(f"ALTER ROLE cassandra WITH PASSWORD = '{cfg.cassandra_admin_password}'")
    log.info("stock superuser password replaced with CASSANDRA_ADMIN_PASSWORD")
    return cluster, session


def roles(cfg: Config) -> dict:
    out = {"loader": (cfg.cassandra_username, cfg.cassandra_password, ["SELECT", "MODIFY"]),
           "reader": (cfg.cassandra_reader_username, cfg.cassandra_reader_password, ["SELECT"])}
    for name, (user, password, _) in out.items():
        if not user or not SAFE_SECRET.match(password or ""):
            raise PipelineError(f"the {name} role needs a username and a password of 12+ letters, digits, "
                                "'.', '_' or '-' in config/pipeline.env: run make setup")
    return out


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("cassandra").setLevel(logging.ERROR)
    cfg = Config.from_env()
    try:
        if cfg.serving_target != "cassandra":
            raise PipelineError("cassandra-init is for the local Cassandra; on AstraDB roles are tokens "
                                "created in the console (README)")
        if not SAFE_SECRET.match(cfg.cassandra_admin_password or ""):
            raise PipelineError("CASSANDRA_ADMIN_PASSWORD is not set: run make setup")
        wanted = roles(cfg)
        cluster, session = admin_session(cfg)
    except Exception as exc:
        log.error("%s", exc)
        return 1
    ks = cfg.cassandra_keyspace
    try:
        for name in ("00_keyspace.cql", "01_tables.cql"):
            for stmt in cql_statements(CQL_DIR / name, ks):
                session.execute(stmt)
        for user, password, permissions in wanted.values():
            session.execute(f"CREATE ROLE IF NOT EXISTS {user} WITH PASSWORD = '{password}' AND LOGIN = true")
            session.execute(f"ALTER ROLE {user} WITH PASSWORD = '{password}'")
            for permission in permissions:
                session.execute(f"GRANT {permission} ON KEYSPACE {ks} TO {user}")
            granted = sorted({r.permission for r in session.execute(f"LIST ALL PERMISSIONS OF {user}")})
            log.info("role %s: %s on keyspace %s", user, ", ".join(granted), ks)
    finally:
        cluster.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
