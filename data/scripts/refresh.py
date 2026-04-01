"""
Quarterly Refresh Script for PJM Dashboard
Run manually each quarter to update data sources.
Outputs a diff summary for review before publishing.

Usage:
  python3 data/scripts/refresh.py

What it does:
  1. Re-fetches EIA generation data (API)
  2. Re-fetches EIA interchange data (API)
  3. Re-fetches data centers from OSM Overpass API
  4. Flags what changed since last run
  5. Reminds you to manually update:
     - PJM queue Excel (download from PJM website)
     - Capacity market prices (from new BRA reports)
     - NERC reserve margins (from new LTRA)

What it does NOT do:
  - Push changes to GitHub
  - Modify index.html
  - Run without your review
"""

import json
import subprocess
import sys
from datetime import date
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent
DATA_DIR = Path(__file__).parent.parent
PROCESSED_DIR = DATA_DIR / "processed"


def get_file_summary(filepath):
    """Get a quick summary of a processed JSON file."""
    try:
        with open(filepath) as f:
            data = json.load(f)
        if "summary" in data:
            return data["summary"]
        if "data" in data and isinstance(data["data"], list):
            return {"record_count": len(data["data"])}
        return {"keys": list(data.keys())}
    except Exception as e:
        return {"error": str(e)}


def snapshot_state():
    """Capture current state of all processed files."""
    state = {}
    for f in PROCESSED_DIR.glob("*.json"):
        state[f.name] = {
            "size": f.stat().st_size,
            "summary": get_file_summary(f),
        }
    for f in PROCESSED_DIR.glob("*.geojson"):
        state[f.name] = {
            "size": f.stat().st_size,
        }
    return state


def run_script(name):
    """Run an ingestion script and return success/failure."""
    script = SCRIPTS_DIR / name
    if not script.exists():
        print(f"  [SKIP] {name} not found")
        return False

    print(f"  Running {name}...")
    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        cwd=str(DATA_DIR.parent),
    )

    if result.returncode != 0:
        print(f"  [FAIL] {name}")
        print(f"    {result.stderr[-200:]}")
        return False

    print(f"  [OK] {name}")
    return True


def compare_states(before, after):
    """Compare before/after snapshots and report changes."""
    changes = []
    for key in sorted(set(list(before.keys()) + list(after.keys()))):
        if key not in before:
            changes.append(f"  [NEW] {key}")
        elif key not in after:
            changes.append(f"  [DELETED] {key}")
        else:
            b = before[key]
            a = after[key]
            if b["size"] != a["size"]:
                delta = a["size"] - b["size"]
                sign = "+" if delta > 0 else ""
                changes.append(f"  [CHANGED] {key}: {b['size']:,} → {a['size']:,} bytes ({sign}{delta:,})")

                # Compare summaries if available
                if "summary" in b and "summary" in a:
                    bs = b["summary"]
                    as_ = a["summary"]
                    if bs != as_:
                        changes.append(f"    Before: {json.dumps(bs, default=str)[:200]}")
                        changes.append(f"    After:  {json.dumps(as_, default=str)[:200]}")
            else:
                changes.append(f"  [UNCHANGED] {key}")

    return changes


def main():
    print("=" * 60)
    print(f"PJM Dashboard Quarterly Refresh — {date.today().isoformat()}")
    print("=" * 60)

    # Snapshot before
    print("\n1. Capturing current state...")
    before = snapshot_state()

    # Run auto-refreshable scripts
    print("\n2. Refreshing API-sourced data...")
    run_script("ingest_eia_generation.py")
    run_script("ingest_interchange.py")
    run_script("ingest_datacenters.py")
    run_script("ingest_plants.py")

    # Snapshot after
    print("\n3. Comparing...")
    after = snapshot_state()
    changes = compare_states(before, after)

    print("\n" + "=" * 60)
    print("DIFF SUMMARY")
    print("=" * 60)
    for line in changes:
        print(line)

    # Manual update reminders
    print("\n" + "=" * 60)
    print("MANUAL UPDATES NEEDED")
    print("=" * 60)
    print("""
  [ ] PJM Queue Excel
      → Go to https://www.pjm.com/planning/services-requests/interconnection-queues
      → Download Serial + Cycle exports
      → Save to data/raw/ (overwrite existing)
      → Run: python3 data/scripts/ingest_queue.py

  [ ] Capacity Market Prices
      → Check https://www.pjm.com/markets-and-operations/rpm for new BRA results
      → If new auction: add entry to ingest_capacity_prices.py
      → Run: python3 data/scripts/ingest_capacity_prices.py

  [ ] NERC Reserve Margins
      → Check https://www.nerc.com/pa/RAPA/ra/Pages/default.aspx for new LTRA
      → If new assessment: update ingest_reserve_margins.py
      → Run: python3 data/scripts/ingest_reserve_margins.py

  [ ] Review all [VERIFY] flags in processed data before publishing
""")

    print("=" * 60)
    print("Review complete. If changes look correct, commit and deploy.")
    print("=" * 60)


if __name__ == "__main__":
    main()
