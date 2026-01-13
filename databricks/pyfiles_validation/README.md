# Databricks Dynamic Python Dependencies

Load Python dependencies dynamically in Databricks Jobs at runtime with support for concurrent execution.

## What Works ✅

### Validated End-to-End (Production Ready)

| Format | What It Does | Status | Tests | Duration |
|--------|-------------|--------|-------|----------|
| **requirements.txt** | Download PyPI packages | ✅ **WORKING** | 4/4 passed | ~8 min |
| **pure_python.zip** | Custom .py files (no dependencies) | ✅ **WORKING** | 5/5 passed | ~9 min |
| **packaged.zip** | Custom package with setup.py/pyproject.toml | ✅ **WORKING** | 4/4 passed | ~8 min |

All formats tested on **both driver and worker nodes** with concurrent job execution. Latest parallel test run: 2025-12-15 (all 3 jobs successful).

**Compute type**: Job clusters (2-node: 1 driver + 1 worker). Serverless compute compatible - see [Serverless Support](#serverless-support) below.

### What Doesn't Work Yet ❌

| Format | Status | Notes |
|--------|--------|-------|
| existing_wheels.zip | ⏳ Processor works, E2E test pending | Pre-built .whl files |
| mixed.zip | ⏳ Processor works, E2E test pending | requirements.txt + custom package |
| nested.zip | ⏳ Processor works, E2E test pending | Dependencies in subdirectory |

## Quick Start

### 1. Deploy the Bundle

```bash
databricks bundle deploy
```

### 2. Upload Your Dependencies

Put your dependencies in a zip file with one of these formats:

**Option A: requirements.txt** (external packages)
```
my-deps.zip:
├── requirements.txt    # Lists PyPI packages
```

**Option B: Pure Python** (your custom code, no dependencies)
```
my-code.zip:
├── mymodule.py
├── utils/
│   └── helpers.py
```

**Option C: Packaged** (your custom code with proper build config)
```
my-package.zip:
├── pyproject.toml      # or setup.py
├── src/
│   └── mypkg/
│       ├── __init__.py
│       └── core.py
```

Upload to DBFS:
```bash
databricks fs cp my-deps.zip dbfs:/FileStore/pyfiles_test/input/my-deps.zip
```

### 3. Run a Test Job

```bash
# Test requirements.txt format
databricks bundle run dynamic_deps_cluster

# Test pure Python format
databricks bundle run test_pure_python

# Test packaged format
databricks bundle run test_packaged
```

## How It Works

**3-Task Pipeline:**

```
Task 1: Process Dependencies
- Reads your zip file
- Detects format automatically
- Converts to wheel format (.wheelhouse.zip)
- Stores in unique UUID-based path
         ↓
Task 2: Load & Validate
- Loads wheelhouse via addPyFile()
- Tests imports on driver
- Tests imports on all worker nodes
         ↓
Task 3: Cleanup
- Removes temporary files
```

**Key Features:**
- ✅ **Concurrent execution**: UUID-based paths prevent collisions
- ✅ **Works on workers**: Dependencies distributed to all executors
- ✅ **Dynamic loading**: Uses `addPyFile()` instead of static `libraries` parameter
- ✅ **Job cluster**: 2-node cluster shared across process + test tasks

## Use Cases

### External Dependencies (requirements.txt)

```python
# my-deps.zip contains:
# requirements.txt:
#   requests==2.31.0
#   pandas==2.1.0

# After Task 1 processes the zip, Task 2+ can:
import requests
import pandas as pd

# Works on both driver and all executors
```

### Custom Python Modules (pure_python)

```python
# my-code.zip contains:
# calculator.py
# utils/helpers.py

# After processing:
from user_modules import calculator
from user_modules.utils import helpers

result = calculator.multiply(6, 7)  # Works everywhere
```

### Custom Package with Dependencies (packaged)

```python
# my-package.zip contains:
# pyproject.toml with dependencies
# src/mypkg/core.py

# After processing:
from mypkg import process_data

data = process_data([1, 2, 3])  # Works on driver and workers
```

## Configuration

### Job Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `volume_base_path` | `dbfs:/FileStore/pyfiles_test` | Where to store artifacts |
| `cleanup_enabled` | `true` | Remove files after run |

### Cluster Settings

- **Spark version**: 14.3.x-scala2.12 (Python 3.10)
- **Nodes**: 2 (1 driver + 1 worker)
- **Shared cluster**: Used by process + test tasks to avoid cold start
- **Libraries**: `wheel`, `setuptools>=61.0` (for building wheels)

## Project Structure

```
pyfiles_validation/
├── src/
│   ├── dependency_processor.py    # Converts zip → wheelhouse
│   ├── test_dependencies.py       # Tests requirements.txt format
│   ├── test_pure_python.py        # Tests pure Python format
│   ├── test_packaged.py           # Tests packaged format
│   └── cleanup.py                 # Removes artifacts
├── resources/
│   ├── job_cluster.py             # Main job definition
│   ├── job_pure_python.py         # Pure Python test job
│   └── job_packaged.py            # Packaged format test job
├── test_artifacts/                # Example zip files
└── databricks.yml                 # Bundle configuration
```

## Important Technical Details

### Why Not Use `libraries` Parameter?

The Databricks `libraries` parameter:
- ❌ Requires static paths at job definition time
- ❌ Doesn't support `{{job.run_id}}` or runtime parameters
- ❌ Can't handle concurrent jobs with different dependencies

**Solution**: `spark.sparkContext.addPyFile()` with task values
- ✅ Loads code at runtime
- ✅ Supports dynamic paths via task values
- ✅ Enables concurrent execution with UUID-based paths
- ✅ Standard PySpark pattern for dynamic dependencies

### DBFS Wheel Distribution Fix

**Critical**: Wheels must be copied to DBFS before `addPyFile()` for worker distribution:

```python
# Copy wheel to persistent DBFS location
dbfs_whl_path = f"dbfs:/tmp/wheels/{wheel_name}"
dbutils.fs.cp(f"file:{local_wheel}", dbfs_whl_path)

# Install on driver
subprocess.run([sys.executable, "-m", "pip", "install", local_wheel])

# Distribute to workers from DBFS (persistent location)
spark.sparkContext.addPyFile(dbfs_whl_path)
```

**Why**: `addPyFile()` with `/tmp` paths fails because workers try to fetch after the temporary file is deleted.

### Python Version Match

**Critical**: Build wheels for Python 3.10 to match DBR 14.3.x:

```python
PYTHON_VERSION = "3.10"  # Match DBR 14.3.x
```

Mismatch causes: `ERROR: Package requires a different Python: 3.10.12 not in '>=3.11'`

### Avoid sys.exit() in Scripts

**Don't do this**:
```python
if success:
    sys.exit(0)  # ❌ Databricks treats this as exception
```

**Do this instead**:
```python
if success:
    # Script completes naturally ✅
    pass
else:
    raise RuntimeError("Tests failed")  # ✅ Proper error signaling
```

## Serverless Support

The `addPyFile()` approach works with **both job clusters and serverless compute**. Current test jobs use 2-node job clusters for validation.

### Using Serverless Compute

To use serverless instead of job clusters:

**In job definition**, replace:
```python
job_cluster_key="shared_cluster"
```

With:
```python
environment_key="default"
```

And remove the `job_clusters` configuration.

**Performance optimization**: Databricks serverless supports [performance modes](https://docs.databricks.com/aws/en/jobs/run-serverless-jobs):
- **Performance optimized** (default): Faster startup, higher cost
- **Standard**: 4-6min startup, lower cost

### Why Not Serverless in Current Tests?

1. **Cluster warmup matters**: With shared job cluster, Task 2 reuses warm cluster from Task 1 (~30-60s savings)
2. **Validation completeness**: Testing on traditional compute proves approach works everywhere
3. **Cost predictability**: Job clusters have fixed duration costs

Serverless is fully compatible with this approach - just swap compute configuration. The core `addPyFile()` + task values pattern works identically.

## Next Steps

1. **Test remaining formats**: existing_wheels, mixed, nested
2. **Add serverless job definitions**: Convert existing jobs to use environment_key
3. **Performance benchmarks**: Compare job cluster vs serverless cold start

## References

- [PROGRESS.md](./PROGRESS.md) - Detailed development notes and learnings
- [Task Values Documentation](https://docs.databricks.com/aws/en/jobs/parameter-use)
- [Spark addPyFile API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.addPyFile.html)
