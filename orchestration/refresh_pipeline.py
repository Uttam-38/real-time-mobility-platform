
import os
import sys
import fcntl
import contextlib
import time
import uuid
import subprocess
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent.parent
PYTHON = ROOT / ".venv" / "bin" / "python"

REFRESH_SECONDS = 300

COMBINED_SCRIPT = ROOT / "orchestration" / "refresh_combined_silver.py"
ENRICHMENT_SCRIPT = ROOT / "spark" / "silver_movement_enrichment.py"
GOLD_SCRIPT = ROOT / "spark" / "gold_analytics.py"

COMBINED_ROOT = ROOT / "data" / "silver_combined_snapshots"
ENRICHED_ROOT = ROOT / "data" / "silver_enriched_snapshots"

HISTORICAL = ROOT / "backups" / "silver_before_recovery_20261029"
LIVE = ROOT / "data" / "silver_dedup"


def log(message):
    timestamp = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    print(f"[{timestamp}] {message}", flush=True)


def run_script(script, *args):
    log(f"Starting: {script.name}")

    command = [str(PYTHON), str(script)]
    command.extend(str(arg) for arg in args)

    subprocess.run(
        command,
        cwd=str(ROOT),
        check=True,
        env=os.environ.copy()
    )

    log(f"Completed: {script.name}")


def refresh_once():
    if not HISTORICAL.is_dir():
        raise FileNotFoundError(
            f"Historical Silver directory missing: {HISTORICAL}"
        )

    if not LIVE.is_dir():
        raise FileNotFoundError(
            f"Live Silver directory missing: {LIVE}"
        )

    before = {
        path.name
        for path in COMBINED_ROOT.iterdir()
        if path.is_dir()
    }

    # 1. Create a new Combined Silver snapshot.
    run_script(COMBINED_SCRIPT)

    after = {
        path.name
        for path in COMBINED_ROOT.iterdir()
        if path.is_dir()
    }

    created = after - before

    if len(created) != 1:
        raise RuntimeError(
            f"Expected one new Combined Silver snapshot, found: {created}"
        )

    snapshot_id = created.pop()
    combined_path = COMBINED_ROOT / snapshot_id

    if not (combined_path / "_SUCCESS").exists():
        raise RuntimeError(
            f"Combined Silver snapshot is incomplete: {combined_path}"
        )

    # 2. Enrich the new Combined Silver snapshot.
    enriched_path = ENRICHED_ROOT / snapshot_id

    run_script(
        ENRICHMENT_SCRIPT,
        combined_path,
        enriched_path
    )

    if not (enriched_path / "_SUCCESS").exists():
        raise RuntimeError(
            f"Enriched Silver snapshot is incomplete: {enriched_path}"
        )

    # 3. Generate and publish a new Gold snapshot.
    run_script(
        GOLD_SCRIPT,
        enriched_path
    )

    log(f"Successfully refreshed snapshot: {snapshot_id}")
    # Clean up old snapshots after successful Gold publication.
    cleanup_script = ROOT / "orchestration" / "cleanup_snapshots.py"

    log("Starting automatic snapshot cleanup")

    run_script(cleanup_script, "--apply")

    log("Automatic snapshot cleanup completed")

def main():
    log("Mobility analytics refresh service started")

    if not PYTHON.exists():
        raise FileNotFoundError(
            f"Python virtual environment not found: {PYTHON}"
        )

    for script in (
        COMBINED_SCRIPT,
        ENRICHMENT_SCRIPT,
        GOLD_SCRIPT
    ):
        if not script.exists():
            raise FileNotFoundError(
                f"Required script not found: {script}"
            )

    COMBINED_ROOT.mkdir(parents=True, exist_ok=True)
    ENRICHED_ROOT.mkdir(parents=True, exist_ok=True)

    while True:
        cycle_start = time.monotonic()

        try:
            log("Starting new analytics refresh cycle")
            refresh_once()
            log("Analytics refresh completed successfully")

        except Exception as error:
            log(f"Analytics refresh failed: {error}")

        elapsed = time.monotonic() - cycle_start
        wait_seconds = max(0, REFRESH_SECONDS - elapsed)

        log(
            f"Waiting {wait_seconds:.0f} seconds "
            "before the next refresh"
        )

        time.sleep(wait_seconds)


@contextlib.contextmanager
def single_instance_lock():
    lock_path = ROOT / "checkpoints" / "analytics_refresh.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    with open(lock_path, "a+") as lock_file:
        try:
            fcntl.flock(
                lock_file.fileno(),
                fcntl.LOCK_EX | fcntl.LOCK_NB
            )
        except BlockingIOError:
            raise RuntimeError(
                "Another analytics refresh service is already running."
            )

        try:
            yield
        finally:
            fcntl.flock(
                lock_file.fileno(),
                fcntl.LOCK_UN
            )


if __name__ == "__main__":
    try:
        with single_instance_lock():
            main()

    except KeyboardInterrupt:
        log("Mobility analytics refresh service stopped")

    except RuntimeError as error:
        log(f"ERROR: {error}")
        sys.exit(1)