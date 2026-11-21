"""Create config/pipeline.env from the example, or complete an existing one, without printing secrets.

python3 scripts/init_env.py    (make setup)

A key that is missing or empty gets its default: random values for the salt and the local Cassandra
passwords, fixed names for the roles. Keys that already have a value are never changed.
"""
import secrets
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "config" / "pipeline.env.example"
TARGET = ROOT / "config" / "pipeline.env"

DEFAULTS = {
    "PII_HASH_SALT": lambda: secrets.token_hex(16),
    "CASSANDRA_USERNAME": lambda: "cpa_loader",
    "CASSANDRA_PASSWORD": lambda: secrets.token_hex(16),
    "CASSANDRA_READER_USERNAME": lambda: "cpa_reader",
    "CASSANDRA_READER_PASSWORD": lambda: secrets.token_hex(16),
    "CASSANDRA_ADMIN_PASSWORD": lambda: secrets.token_hex(16),
}


def main():
    if not TARGET.exists():
        shutil.copy(EXAMPLE, TARGET)
        TARGET.chmod(0o600)
    lines = TARGET.read_text().splitlines()
    seen = {}
    for i, line in enumerate(lines):
        key = line.split("=", 1)[0].strip() if "=" in line and not line.lstrip().startswith("#") else None
        if key in DEFAULTS:
            seen[key] = i
    filled = []
    for key, make in DEFAULTS.items():
        if key not in seen:
            lines.append(f"{key}={make()}")
            filled.append(key)
        elif not lines[seen[key]].split("=", 1)[1].strip():
            lines[seen[key]] = f"{key}={make()}"
            filled.append(key)
    TARGET.write_text("\n".join(lines) + "\n")
    print(f"{TARGET.relative_to(ROOT)}: filled {', '.join(filled) if filled else 'nothing, all keys set'}")


if __name__ == "__main__":
    main()
