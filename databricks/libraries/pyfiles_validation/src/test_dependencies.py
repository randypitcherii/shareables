#!/usr/bin/env python3
"""
Test script to verify dependencies are available on driver and workers.

This script validates that dependencies from the wheelhouse are correctly
installed and accessible in both the driver process and executor processes.
"""
import sys
from typing import List, Tuple


def test_driver_imports() -> List[Tuple[str, bool, str]]:
    """
    Test that dependencies can be imported on the driver.

    Returns:
        List of (test_name, success, message) tuples
    """
    results = []

    # Test 1: Import requests
    print("[Driver] Testing requests import...")
    try:
        import requests
        version = requests.__version__
        results.append(("requests", True, f"version {version}"))
        print(f"  ✓ requests {version}")
    except ImportError as e:
        results.append(("requests", False, str(e)))
        print(f"  ✗ Failed: {e}")

    # Test 2: Import python-dateutil
    print("[Driver] Testing dateutil import...")
    try:
        import dateutil
        from dateutil.parser import parse
        # Quick functional test
        test_date = parse("2025-01-15")
        results.append(("dateutil", True, "functional test passed"))
        print(f"  ✓ dateutil (parsed test date: {test_date.date()})")
    except ImportError as e:
        results.append(("dateutil", False, str(e)))
        print(f"  ✗ Failed: {e}")

    return results


def test_worker_imports() -> List[Tuple[str, bool, str]]:
    """
    Test that dependencies can be imported on Spark executors.

    Uses mapPartitions to force execution on worker nodes.

    Returns:
        List of (test_name, success, message) tuples
    """
    from pyspark.sql import SparkSession

    print("[Workers] Initializing Spark session...")
    spark = SparkSession.builder.getOrCreate()
    print(f"  Spark version: {spark.version}")

    # Create a small DataFrame to distribute work to executors
    # repartition forces data to be distributed across available executors
    num_partitions = spark.sparkContext.defaultParallelism
    print(f"  Default parallelism: {num_partitions}")

    df = spark.range(num_partitions * 2).repartition(num_partitions)

    results = []

    # Test 1: Import requests on workers
    print("[Workers] Testing requests import on executors...")

    def test_requests_on_executor(iterator):
        """Function that runs on each executor."""
        try:
            import requests
            version = requests.__version__
            # Return success for each partition
            yield f"executor_ok:requests={version}"
        except ImportError as e:
            yield f"executor_fail:requests={str(e)}"

    worker_results = df.rdd.mapPartitions(test_requests_on_executor).collect()
    successes = [r for r in worker_results if r.startswith("executor_ok")]
    failures = [r for r in worker_results if r.startswith("executor_fail")]

    if failures:
        results.append(("requests_workers", False, f"{len(failures)} executors failed"))
        print(f"  ✗ Failed on {len(failures)} executor(s)")
        for failure in failures:
            print(f"    {failure}")
    else:
        results.append(("requests_workers", True, f"{len(successes)} executors OK"))
        print(f"  ✓ Success on {len(successes)} executor(s)")

    # Test 2: Import dateutil on workers
    print("[Workers] Testing dateutil import on executors...")

    def test_dateutil_on_executor(iterator):
        """Function that runs on each executor."""
        try:
            from dateutil.parser import parse
            # Quick functional test
            test_date = parse("2025-01-15")
            yield f"executor_ok:dateutil={test_date.date()}"
        except ImportError as e:
            yield f"executor_fail:dateutil={str(e)}"

    worker_results = df.rdd.mapPartitions(test_dateutil_on_executor).collect()
    successes = [r for r in worker_results if r.startswith("executor_ok")]
    failures = [r for r in worker_results if r.startswith("executor_fail")]

    if failures:
        results.append(("dateutil_workers", False, f"{len(failures)} executors failed"))
        print(f"  ✗ Failed on {len(failures)} executor(s)")
        for failure in failures:
            print(f"    {failure}")
    else:
        results.append(("dateutil_workers", True, f"{len(successes)} executors OK"))
        print(f"  ✓ Success on {len(successes)} executor(s)")

    return results


def main():
    """Run all dependency tests."""
    print("=" * 70)
    print("Dependency Validation Test")
    print("=" * 70)
    print()

    # Load wheelhouse using addPyFile (dynamic runtime loading)
    print("Phase 0: Loading wheelhouse dynamically")
    print("-" * 70)
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        # Get wheelhouse path from Task 1's task value
        try:
            # Use Python dbutils API (not Java bridge)
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)

            # Python API supports keyword arguments
            wheelhouse_path = dbutils.jobs.taskValues.get(
                taskKey="process_deps",
                key="wheelhouse_path",
                debugValue=""  # Return empty string if running outside job for debugging
            )

            if not wheelhouse_path or str(wheelhouse_path) == "":
                raise RuntimeError("wheelhouse_path task value not found from process_deps task")

            print(f"  Wheelhouse path from task value: {wheelhouse_path}")

            # Add wheelhouse to Python path for driver and executors
            spark.sparkContext.addPyFile(wheelhouse_path)
            print(f"  ✓ Added wheelhouse to Spark context via addPyFile()")
            print(f"  Dependencies now available on driver and all executors")

        except Exception as e:
            print(f"  ✗ Failed to load wheelhouse: {e}")
            raise RuntimeError(f"Cannot load dependencies: {e}")

    except Exception as e:
        print(f"  ✗ Failed to initialize Spark: {e}")
        raise
    print()

    all_results = []

    # Test driver
    print("Phase 1: Testing dependencies on driver")
    print("-" * 70)
    driver_results = test_driver_imports()
    all_results.extend(driver_results)
    print()

    # Test workers
    print("Phase 2: Testing dependencies on Spark executors")
    print("-" * 70)
    worker_results = test_worker_imports()
    all_results.extend(worker_results)
    print()

    # Summary
    print("=" * 70)
    print("Test Summary")
    print("=" * 70)

    successes = [r for r in all_results if r[1]]
    failures = [r for r in all_results if not r[1]]

    for test_name, success, message in all_results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status:8} {test_name:20} {message}")

    print("-" * 70)
    print(f"Total: {len(successes)}/{len(all_results)} passed")

    if failures:
        print()
        print("FAILED: Some dependencies were not available")
        raise RuntimeError(f"{len(failures)} test(s) failed")
    else:
        print()
        print("SUCCESS: All dependencies loaded correctly on driver and workers!")


if __name__ == "__main__":
    main()
