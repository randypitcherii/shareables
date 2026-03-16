#!/usr/bin/env python3
"""
Test script for pure Python module format.

Validates that pure .py modules wrapped in user_modules wheel work correctly
on both driver and executor nodes.
"""
import sys
from typing import List, Tuple


def test_driver_imports() -> List[Tuple[str, bool, str]]:
    """
    Test that user modules can be imported on the driver.

    Returns:
        List of (test_name, success, message) tuples
    """
    results = []

    # Test 1: Import calculator module
    print("[Driver] Testing calculator import...")
    try:
        from user_modules import calculator
        result = calculator.multiply(6, 7)
        expected = 42
        if result == expected:
            results.append(("calculator.multiply", True, f"multiply(6,7) = {result}"))
            print(f"  ✓ calculator.multiply works: {result}")
        else:
            results.append(("calculator.multiply", False, f"Expected {expected}, got {result}"))
            print(f"  ✗ Wrong result: {result}")
    except Exception as e:
        results.append(("calculator.multiply", False, str(e)))
        print(f"  ✗ Failed: {e}")

    # Test 2: Import helpers module
    print("[Driver] Testing helpers import...")
    try:
        from user_modules.myutils import helpers
        result = helpers.add(10, 5)
        expected = 15
        if result == expected:
            results.append(("helpers.add", True, f"add(10,5) = {result}"))
            print(f"  ✓ helpers.add works: {result}")
        else:
            results.append(("helpers.add", False, f"Expected {expected}, got {result}"))
            print(f"  ✗ Wrong result: {result}")
    except Exception as e:
        results.append(("helpers.add", False, str(e)))
        print(f"  ✗ Failed: {e}")

    # Test 3: Test greet function
    print("[Driver] Testing helpers.greet...")
    try:
        from user_modules.myutils import helpers
        result = helpers.greet("World")
        expected = "Hello, World!"
        if result == expected:
            results.append(("helpers.greet", True, f"greet returned correct string"))
            print(f"  ✓ helpers.greet works: '{result}'")
        else:
            results.append(("helpers.greet", False, f"Expected '{expected}', got '{result}'"))
            print(f"  ✗ Wrong result: '{result}'")
    except Exception as e:
        results.append(("helpers.greet", False, str(e)))
        print(f"  ✗ Failed: {e}")

    return results


def test_worker_imports() -> List[Tuple[str, bool, str]]:
    """
    Test that user modules work on Spark executors.

    Uses mapPartitions to force execution on worker nodes.

    Returns:
        List of (test_name, success, message) tuples
    """
    from pyspark.sql import SparkSession

    print("[Workers] Initializing Spark session...")
    spark = SparkSession.builder.getOrCreate()
    print(f"  Spark version: {spark.version}")

    num_partitions = spark.sparkContext.defaultParallelism
    print(f"  Default parallelism: {num_partitions}")

    df = spark.range(num_partitions * 2).repartition(num_partitions)

    results = []

    # Test 1: calculator.multiply on workers
    print("[Workers] Testing calculator.multiply on executors...")

    def test_calculator_on_executor(iterator):
        """Function that runs on each executor."""
        try:
            from user_modules import calculator
            result = calculator.multiply(8, 5)
            if result == 40:
                yield f"executor_ok:calculator=multiply(8,5)={result}"
            else:
                yield f"executor_fail:calculator=wrong_result_{result}"
        except Exception as e:
            yield f"executor_fail:calculator={str(e)}"

    worker_results = df.rdd.mapPartitions(test_calculator_on_executor).collect()
    successes = [r for r in worker_results if r.startswith("executor_ok")]
    failures = [r for r in worker_results if r.startswith("executor_fail")]

    if failures:
        results.append(("calculator_workers", False, f"{len(failures)} executors failed"))
        print(f"  ✗ Failed on {len(failures)} executor(s)")
        for failure in failures:
            print(f"    {failure}")
    else:
        results.append(("calculator_workers", True, f"{len(successes)} executors OK"))
        print(f"  ✓ Success on {len(successes)} executor(s)")

    # Test 2: helpers.add on workers
    print("[Workers] Testing helpers.add on executors...")

    def test_helpers_on_executor(iterator):
        """Function that runs on each executor."""
        try:
            from user_modules.myutils import helpers
            result = helpers.add(100, 50)
            if result == 150:
                yield f"executor_ok:helpers=add(100,50)={result}"
            else:
                yield f"executor_fail:helpers=wrong_result_{result}"
        except Exception as e:
            yield f"executor_fail:helpers={str(e)}"

    worker_results = df.rdd.mapPartitions(test_helpers_on_executor).collect()
    successes = [r for r in worker_results if r.startswith("executor_ok")]
    failures = [r for r in worker_results if r.startswith("executor_fail")]

    if failures:
        results.append(("helpers_workers", False, f"{len(failures)} executors failed"))
        print(f"  ✗ Failed on {len(failures)} executor(s)")
        for failure in failures:
            print(f"    {failure}")
    else:
        results.append(("helpers_workers", True, f"{len(successes)} executors OK"))
        print(f"  ✓ Success on {len(successes)} executor(s)")

    return results


def main():
    """Run all tests for pure Python modules."""
    print("=" * 70)
    print("Pure Python Module Validation Test")
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
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)

            wheelhouse_path = dbutils.jobs.taskValues.get(
                taskKey="process_deps",
                key="wheelhouse_path",
                debugValue=""
            )

            if not wheelhouse_path or str(wheelhouse_path) == "":
                raise RuntimeError("wheelhouse_path task value not found from process_deps task")

            print(f"  Wheelhouse path from task value: {wheelhouse_path}")

            # For wheels, we need to pip install them, not addPyFile
            # Extract and install wheels from wheelhouse
            import subprocess
            import tempfile
            import zipfile
            from pathlib import Path

            # Copy wheelhouse locally if it's in DBFS
            if wheelhouse_path.startswith("dbfs:/"):
                local_wh = f"/tmp/{Path(wheelhouse_path).name}"
                dbutils.fs.cp(wheelhouse_path, f"file:{local_wh}")
                wheelhouse_path = local_wh

            # Extract wheels and install
            with tempfile.TemporaryDirectory() as tmpdir:
                with zipfile.ZipFile(wheelhouse_path, 'r') as zf:
                    zf.extractall(tmpdir)

                # Find all wheels
                wheels = list(Path(tmpdir).glob("*.whl"))
                if not wheels:
                    raise RuntimeError("No wheels found in wheelhouse")

                print(f"  Found {len(wheels)} wheel(s) in wheelhouse")

                # Install wheels on driver AND distribute to workers
                for whl in wheels:
                    print(f"  Processing: {whl.name}")

                    # Copy wheel to DBFS for persistent storage (addPyFile needs persistent path)
                    dbfs_whl_path = f"dbfs:/tmp/wheels/{whl.name}"
                    print(f"    Copying to DBFS: {dbfs_whl_path}")
                    dbutils.fs.cp(f"file:{str(whl)}", dbfs_whl_path, recurse=True)

                    # Install on driver
                    result = subprocess.run(
                        [sys.executable, "-m", "pip", "install", "--quiet", str(whl)],
                        capture_output=True,
                        text=True
                    )
                    if result.returncode != 0:
                        print(f"    Warning: pip install had issues: {result.stderr}")
                    else:
                        print(f"    ✓ Installed on driver")

                    # Distribute to workers from DBFS (persistent location)
                    spark.sparkContext.addPyFile(dbfs_whl_path)
                    print(f"    ✓ Distributed to workers via addPyFile")

                print(f"  ✓ All wheels installed on driver and distributed to workers")
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
    print("Phase 1: Testing user modules on driver")
    print("-" * 70)
    driver_results = test_driver_imports()
    all_results.extend(driver_results)
    print()

    # Test workers
    print("Phase 2: Testing user modules on Spark executors")
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
        print(f"{status:8} {test_name:25} {message}")

    print("-" * 70)
    print(f"Total: {len(successes)}/{len(all_results)} passed")

    if failures:
        print()
        print("FAILED: Some user modules were not available")
        raise RuntimeError(f"{len(failures)} test(s) failed")
    else:
        print()
        print("SUCCESS: All user modules loaded correctly on driver and workers!")


if __name__ == "__main__":
    main()
