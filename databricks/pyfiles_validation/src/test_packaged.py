"""Test packaged Python packages (with pyproject.toml/setup.py)."""
import sys
import subprocess
import zipfile
from pathlib import Path
from typing import List, Tuple
from pyspark.sql import SparkSession


def test_driver_imports() -> List[Tuple[str, bool, str]]:
    """Test that packaged modules can be imported on the driver."""
    results = []

    # Test 1: Import testpkg and process numeric data
    try:
        from testpkg import process_data
        result = process_data([1, 2, 3, 4, 5])
        expected = {"count": 5, "sum": 15, "items": [1, 2, 3, 4, 5]}
        success = result == expected
        results.append(("testpkg.process_data", success, str(result)))
        if success:
            print(f"  ✓ process_data([1,2,3,4,5]) = {result}")
        else:
            print(f"  ✗ process_data([1,2,3,4,5]) = {result}, expected {expected}")
    except Exception as e:
        results.append(("testpkg.process_data", False, str(e)))
        print(f"  ✗ Failed to test process_data: {e}")

    # Test 2: Process string data
    try:
        from testpkg import process_data
        result = process_data(["a", "b", "c"])
        expected = {"count": 3, "sum": None, "items": ["a", "b", "c"]}
        success = result == expected
        results.append(("testpkg.process_strings", success, str(result)))
        if success:
            print(f"  ✓ process_data(['a','b','c']) = {result}")
        else:
            print(f"  ✗ process_data(['a','b','c']) = {result}, expected {expected}")
    except Exception as e:
        results.append(("testpkg.process_strings", False, str(e)))
        print(f"  ✗ Failed to test process_data with strings: {e}")

    # Test 3: Process empty list
    try:
        from testpkg import process_data
        result = process_data([])
        expected = {"count": 0, "sum": None, "items": []}
        success = result == expected
        results.append(("testpkg.process_empty", success, str(result)))
        if success:
            print(f"  ✓ process_data([]) = {result}")
        else:
            print(f"  ✗ process_data([]) = {result}, expected {expected}")
    except Exception as e:
        results.append(("testpkg.process_empty", False, str(e)))
        print(f"  ✗ Failed to test process_data with empty list: {e}")

    return results


def test_worker_imports(spark: SparkSession) -> List[Tuple[str, bool, str]]:
    """Test that packaged modules can be imported on Spark executors."""
    results = []

    # Test 1: Process data on executors
    try:
        print(f"  Spark version: {spark.version}")
        print(f"  Default parallelism: {spark.sparkContext.defaultParallelism}")

        def test_on_executor(_):
            """Test that runs on each executor."""
            from testpkg import process_data
            result = process_data([10, 20, 30])
            expected = {"count": 3, "sum": 60, "items": [10, 20, 30]}
            return result == expected

        # Run test on all executors
        rdd = spark.sparkContext.parallelize(range(4), 4)
        executor_results = rdd.map(test_on_executor).collect()

        success = all(executor_results)
        success_count = sum(executor_results)

        results.append(("testpkg_workers", success, f"{success_count}/{len(executor_results)} executors"))

        if success:
            print(f"  ✓ Success on {len(executor_results)} executor(s)")
        else:
            print(f"  ✗ Only {success_count}/{len(executor_results)} executors succeeded")

    except Exception as e:
        results.append(("testpkg_workers", False, str(e)))
        print(f"  ✗ Failed to test on executors: {e}")

    return results


def main():
    """Main test execution."""
    print("=" * 70)
    print("Packaged Format Validation Test")
    print("=" * 70)

    # Phase 0: Load dependencies
    print("\nPhase 0: Loading wheelhouse dynamically")
    print("-" * 70)

    try:
        from pyspark.dbutils import DBUtils
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)

        # Get wheelhouse path from task value
        wheelhouse_path = dbutils.jobs.taskValues.get(
            taskKey="process_deps",
            key="wheelhouse_path",
            debugValue="dbfs:/FileStore/pyfiles_test/wheelhouse.zip"
        )
        print(f"  Wheelhouse path from task value: {wheelhouse_path}")

        # Phase 0.1: Load dependencies
        try:
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
        print(f"✗ FATAL: Cannot initialize Spark/dbutils: {e}")
        raise RuntimeError(f"Cannot initialize Spark/dbutils: {e}")

    # Phase 1: Test on driver
    print("\nPhase 1: Testing packaged module on driver")
    print("-" * 70)
    print("[Driver] Testing testpkg import...")

    driver_results = test_driver_imports()

    # Phase 2: Test on workers
    print("\nPhase 2: Testing packaged module on Spark executors")
    print("-" * 70)
    print("[Workers] Initializing Spark session...")

    worker_results = test_worker_imports(spark)

    # Summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)

    all_results = driver_results + worker_results

    for test_name, success, result in all_results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status:8} {test_name:25} {result}")

    print("-" * 70)
    passed = sum(1 for _, success, _ in all_results if success)
    total = len(all_results)
    print(f"Total: {passed}/{total} passed")

    if passed == total:
        print("\nSUCCESS: All packaged modules loaded correctly on driver and workers!")
        # Script completes successfully
    else:
        print(f"\nFAILURE: {total - passed} test(s) failed")
        raise RuntimeError(f"{total - passed} test(s) failed")


if __name__ == "__main__":
    main()
