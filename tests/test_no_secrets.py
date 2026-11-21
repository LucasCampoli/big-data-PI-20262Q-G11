"""No credentials in the repository (brief §7.6, §9.3): runs with make test, on every tracked file."""
import re
import subprocess

import pytest

from pipeline.config import ENV_FILE, REPO_ROOT

SECRET_KEYS = ("ASTRA_TOKEN", "PII_HASH_SALT", "CASSANDRA_PASSWORD", "CASSANDRA_READER_PASSWORD",
               "CASSANDRA_ADMIN_PASSWORD")
PATTERNS = {
    "AstraDB token": re.compile(r"AstraCS:[A-Za-z0-9]{8,}"),
    "private key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    # KEY=value with a real value; empty and <placeholder> values are fine.
    "secret assignment": re.compile(rf"\b({'|'.join(SECRET_KEYS)})[ \t]*=[ \t]*['\"]?(?![<'\"\s$])[^\s'\"]{{6,}}"),
}


def tracked_files():
    try:
        out = subprocess.run(["git", "-C", str(REPO_ROOT), "ls-files", "-z"], capture_output=True, check=True)
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("not a git checkout")
    paths = [REPO_ROOT / p for p in out.stdout.decode().split("\0") if p]
    return [p for p in paths if p.is_file() and p.suffix not in {".pdf", ".png", ".zip", ".parquet"}]


def local_secrets():
    """The values of config/pipeline.env, which is ignored by git, so none can leak into a tracked file."""
    if not ENV_FILE.exists():
        return []
    values = []
    for line in ENV_FILE.read_text().splitlines():
        key, _, value = line.partition("=")
        if key.strip() in SECRET_KEYS and len(value.strip()) >= 8:
            values.append(value.strip().strip("'\""))
    return values


def test_no_secret_in_tracked_files():
    secrets = local_secrets()
    hits = []
    for path in tracked_files():
        text = path.read_text(errors="ignore")
        hits += [f"{path.relative_to(REPO_ROOT)}: {name}" for name, rx in PATTERNS.items() if rx.search(text)]
        # The file and the line, never the value.
        hits += [f"{path.relative_to(REPO_ROOT)}: a value of config/pipeline.env" for s in secrets if s in text]
    assert not hits, hits


def test_bundle_and_env_file_are_ignored():
    for name in ("config/pipeline.env", "secure-connect-cpa.zip"):
        ignored = subprocess.run(["git", "-C", str(REPO_ROOT), "check-ignore", "-q", name]).returncode == 0
        assert ignored, name
