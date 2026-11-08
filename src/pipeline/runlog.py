"""Run-scoped logging: one log file and one JSON summary per run, both keyed by run_id."""
import json
import logging
import subprocess
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .config import REPO_ROOT, Config

log = logging.getLogger("pipeline")


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


def git_commit() -> str:
    try:
        out = subprocess.run(["git", "-C", str(REPO_ROOT), "rev-parse", "--short", "HEAD"],
                             capture_output=True, text=True, timeout=10)
        # evidence/ is what the runs write: make rerun-evidence updates it between its two runs,
        # and that must not mark the second run as built from modified code.
        dirty = subprocess.run(["git", "-C", str(REPO_ROOT), "status", "--porcelain", "--untracked-files=no",
                                "--", ".", ":(exclude)evidence"],
                               capture_output=True, text=True, timeout=10).stdout.strip()
        return out.stdout.strip() + ("-dirty" if dirty else "")
    except (OSError, subprocess.SubprocessError):
        return "unknown"


class _RunIdFilter(logging.Filter):
    def __init__(self, run_id: str):
        super().__init__()
        self.run_id = run_id

    def filter(self, record):
        record.run_id = self.run_id
        return True


def setup_logging(cfg: Config, run_id: str) -> Path:
    cfg.logs.mkdir(parents=True, exist_ok=True)
    log_file = cfg.logs / f"{run_id}.log"
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(run_id)s %(name)s: %(message)s",
                            datefmt="%Y-%m-%dT%H:%M:%S")
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    for handler in (logging.StreamHandler(), logging.FileHandler(log_file)):
        handler.setFormatter(fmt)
        handler.addFilter(_RunIdFilter(run_id))
        root.addHandler(handler)
    # py4j logs every gateway call at INFO, which buries the pipeline's own lines.
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("cassandra").setLevel(logging.WARNING)
    return log_file


class RunSummary:
    """Collects stage timings and metrics, and writes logs/<run_id>.json at the end of the run."""

    def __init__(self, cfg: Config, run_id: str, stages: list):
        self.cfg = cfg
        self.data = {
            "run_id": run_id,
            "started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "git_commit": git_commit(),
            "requested_stages": stages,
            "config": cfg.masked(),
            "stages": [],
            "status": "running",
        }
        self.current = None

    @contextmanager
    def stage(self, name: str):
        entry = {"stage": name, "status": "running", "metrics": {}}
        self.data["stages"].append(entry)
        self.current = entry
        start = time.monotonic()
        log.info("stage %s: start", name)
        try:
            yield entry["metrics"]
            entry["status"] = "ok"
        except Exception as exc:
            entry["status"] = "failed"
            entry["error"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            entry["seconds"] = round(time.monotonic() - start, 2)
            log.info("stage %s: %s in %.1f s", name, entry["status"], entry["seconds"])

    def set(self, key: str, value):
        self.data[key] = value

    def write(self, status: str) -> Path:
        self.data["status"] = status
        self.data["finished_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        path = self.cfg.logs / f"{self.data['run_id']}.json"
        path.write_text(json.dumps(self.data, indent=2, default=str) + "\n")
        return path
