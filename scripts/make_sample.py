"""Cut a small, deterministic sample of Landing into data/sample/.

Run from the repo root: python scripts/make_sample.py
Keeps every row of the first N_ORGS organizations (sorted by org_id) in each CSV, and their events
from the first N_PARTS JSONL parts. Lines are copied byte for byte, so the sample keeps the quality
issues of the original files. Re-running gives the same output.
"""
import csv, json, os, shutil, sys
from pathlib import Path

N_ORGS = 20
N_PARTS = 10

ROOT = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
LANDING = Path(os.environ.get("DATA_LANDING", ROOT / "data" / "landing"))
SAMPLE = ROOT / "data" / "sample"
EVENTS = "usage_events_stream"

with open(LANDING / "customers_orgs.csv", newline="") as f:
    orgs = set(sorted(row["org_id"] for row in csv.DictReader(f))[:N_ORGS])

if SAMPLE.exists():
    shutil.rmtree(SAMPLE)
(SAMPLE / EVENTS).mkdir(parents=True)

for path in sorted(LANDING.glob("*.csv")):
    with open(path, newline="") as f:
        lines = f.read().splitlines(keepends=True)
    header, rows = lines[0], lines[1:]
    org_col = next(csv.reader([header])).index("org_id")
    kept = [r for r in rows if next(csv.reader([r]))[org_col] in orgs]
    (SAMPLE / path.name).write_text(header + "".join(kept))
    print(f"{path.name}: {len(kept)} of {len(rows)} rows")

for path in sorted((LANDING / EVENTS).glob("*.jsonl"))[:N_PARTS]:
    lines = path.read_text().splitlines(keepends=True)
    kept = [l for l in lines if json.loads(l)["org_id"] in orgs]
    (SAMPLE / EVENTS / path.name).write_text("".join(kept))
    print(f"{EVENTS}/{path.name}: {len(kept)} of {len(lines)} events")
