import subprocess
import argparse
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"

COMBINED = DATA / "silver_combined_snapshots"
ENRICHED = DATA / "silver_enriched_snapshots"
GOLD = DATA / "gold_snapshots"

KEEP_LATEST = 5


def current_gold_snapshot():
    manifest_path = GOLD / "current.json"

    if not manifest_path.exists():
        return None

    with open(manifest_path, encoding="utf-8") as file:
        manifest = json.load(file)

    snapshot_id = manifest.get("snapshot_id")

    if not isinstance(snapshot_id, str):
        raise ValueError("Invalid Gold snapshot ID")

    if (
        not snapshot_id
        or snapshot_id in (".", "..")
        or "/" in snapshot_id
        or "\\" in snapshot_id
    ):
        raise ValueError("Unsafe Gold snapshot ID")

    return snapshot_id


def cleanup_directory(directory, protected=None, dry_run=True):
    protected = protected or set()

    if not directory.exists():
        print(f"Directory missing: {directory}")
        return

    snapshots = sorted(
        [
            path for path in directory.iterdir()
            if path.is_dir() and not path.is_symlink()
        ],
        key=lambda path: path.stat().st_mtime,
        reverse=True
    )

    keep = {
        path.name for path in snapshots[:KEEP_LATEST]
    } | protected

    print(f"\nChecking: {directory.name}")
    print(f"Found {len(snapshots)} snapshots")

    for snapshot in snapshots:
        if snapshot.name in keep:
            print(f"KEEP: {snapshot.name}")
            continue

        if dry_run:
            print(f"WOULD DELETE: {snapshot.name}")
        else:
            shutil.rmtree(snapshot)
            print(f"DELETED: {snapshot.name}")

def dashboard_is_running():
    """Check whether the Streamlit dashboard process is running."""
    result = subprocess.run(
        ["pgrep", "-f", "streamlit run dashboard/app.py"],
        capture_output=True,
        text=True,
        check=False
    )
    return result.returncode == 0

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete old snapshots"
    )

    args = parser.parse_args()

    dry_run = not args.apply

    print("Snapshot cleanup")
    print("Mode:", "DRY RUN" if dry_run else "APPLY")
    print("Keeping latest:", KEEP_LATEST)

    protected_gold = current_gold_snapshot()

    cleanup_directory(
        COMBINED,
        dry_run=dry_run
    )

    cleanup_directory(
        ENRICHED,
        dry_run=dry_run
    )

    if dashboard_is_running():
        print(
        "\nDashboard is running. "
        "Skipping Gold snapshot deletion for safety."
        )
    else:
        cleanup_directory(
            GOLD,
            protected={protected_gold} if protected_gold else set(),
            dry_run=dry_run
        )


if __name__ == "__main__":
    main()
