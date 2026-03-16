#!/usr/bin/env python3
"""
Cleanup script for removing temporary job artifacts.

Deletes run_id directory containing wheelhouse output.
Reads run_id from task value set by process_deps task.
"""
import argparse
import sys
import shutil
from pathlib import Path


def cleanup_artifacts(volume_base: str, cleanup_enabled: bool) -> None:
    """
    Clean up artifacts for a specific run.

    Args:
        volume_base: Base volume path (e.g., dbfs:/FileStore/pyfiles_test)
        cleanup_enabled: Whether to actually perform cleanup
    """
    from pyspark.sql import SparkSession
    from pyspark.dbutils import DBUtils

    print("=" * 60)
    print("Cleanup Task")
    print("=" * 60)
    print(f"Volume base: {volume_base}")
    print(f"Cleanup enabled: {cleanup_enabled}")

    # Get run_id from task value
    try:
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        run_id = dbutils.jobs.taskValues.get(
            taskKey="process_deps",
            key="run_id",
            debugValue=""
        )
        if not run_id:
            print("WARNING: Could not get run_id from task value, skipping cleanup")
            return
        print(f"Run ID: {run_id}")
    except Exception as e:
        print(f"WARNING: Failed to get run_id from task value: {e}")
        print("INFO: Skipping cleanup")
        return

    if not cleanup_enabled:
        print("INFO: Cleanup disabled, preserving artifacts for debugging")
        return

    # Construct run directory path
    run_dir = f"{volume_base}/{run_id}"

    # Convert dbfs:/ to /dbfs/ for local filesystem access
    def to_local_path(path: str) -> str:
        return path.replace("dbfs:/", "/dbfs/")

    local_run_dir = Path(to_local_path(run_dir))

    if not local_run_dir.exists():
        print(f"INFO: Run directory does not exist, nothing to clean: {run_dir}")
        return

    try:
        print(f"INFO: Removing run directory: {run_dir}")
        shutil.rmtree(local_run_dir)
        print(f"  ✓ Removed: {run_dir}")
        print("-" * 60)
        print("SUCCESS: Cleaned up run artifacts")
    except Exception as e:
        print(f"WARNING: Failed to remove {run_dir}: {e}")
        # Don't fail the job if cleanup fails
        print("INFO: Continuing despite cleanup failure")


def main():
    parser = argparse.ArgumentParser(description="Clean up job artifacts")
    parser.add_argument("volume_base", help="Base volume path")
    parser.add_argument(
        "--cleanup-enabled",
        type=lambda x: x.lower() in ("true", "1", "yes"),
        default=True,
        help="Whether to perform cleanup (default: true)",
    )

    args = parser.parse_args()

    cleanup_artifacts(args.volume_base, args.cleanup_enabled)
    print("Cleanup task completed")


if __name__ == "__main__":
    main()
