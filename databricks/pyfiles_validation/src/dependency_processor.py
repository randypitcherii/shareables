#!/usr/bin/env python3
"""
Dependency Processor for Databricks Jobs

Converts user-provided zip files containing Python dependencies into
.wheelhouse.zip format that can be attached via the Databricks Jobs API
`libraries` parameter.

Supported input formats:
1. requirements.txt in zip root -> pip wheel all deps
2. setup.py or pyproject.toml in zip -> pip wheel builds the package
3. Pre-built .whl files in zip -> copy directly to wheelhouse
4. Pure .py modules (no packaging) -> create minimal wheel

Output: .wheelhouse.zip file in specified UC Volume location
"""
import os
import sys
import zipfile
import subprocess
import tempfile
import shutil
import hashlib
import logging
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class DependencyProcessor:
    """Process user zip files into wheelhouse format."""

    # Platform settings for Databricks workers
    PLATFORM = "manylinux2014_x86_64"
    PYTHON_VERSION = "3.10"  # Match DBR 14.3.x (Python 3.10)

    def __init__(self, input_zip_path: str, output_volume_path: str, run_id: str = None):
        """
        Initialize processor.

        Args:
            input_zip_path: Path to user's zip file (UC Volume path)
            output_volume_path: UC Volume path for output wheelhouse
            run_id: Unique run identifier for path isolation (generated if not provided)
        """
        import uuid
        self.input_zip_path = input_zip_path
        self.output_volume_path = output_volume_path.rstrip('/')
        self.run_id = run_id or str(uuid.uuid4())

    def process(self) -> str:
        """
        Process the input zip and create wheelhouse.

        Returns:
            Path to the created .wheelhouse.zip file
        """
        logger.info(f"Processing: {self.input_zip_path}")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            extract_dir = tmpdir / "extracted"
            wheel_dir = tmpdir / "wheels"
            extract_dir.mkdir()
            wheel_dir.mkdir()

            # Extract user's zip
            self._extract_zip(self.input_zip_path, extract_dir)

            # Detect format and process accordingly
            processed = False

            # Strategy 1: requirements.txt
            if self._process_requirements(extract_dir, wheel_dir):
                processed = True

            # Strategy 2: setup.py or pyproject.toml (Python package)
            if self._process_package(extract_dir, wheel_dir):
                processed = True

            # Strategy 3: Pre-built wheels
            if self._copy_existing_wheels(extract_dir, wheel_dir):
                processed = True

            # Strategy 4: Pure Python modules (fallback)
            if not processed:
                if self._process_pure_python(extract_dir, wheel_dir):
                    processed = True

            if not processed:
                raise ValueError(
                    f"Could not process zip file. Expected one of: "
                    f"requirements.txt, setup.py, pyproject.toml, .whl files, or .py modules"
                )

            # Check we have wheels
            wheels = list(wheel_dir.glob("*.whl"))
            if not wheels:
                raise ValueError("No wheel files were generated")

            logger.info(f"Generated {len(wheels)} wheel(s)")
            for whl in wheels:
                logger.info(f"  - {whl.name}")

            # Create wheelhouse.zip
            output_path = self._create_wheelhouse(wheel_dir)
            return output_path

    def _extract_zip(self, zip_path: str, dest: Path) -> None:
        """Extract zip file to destination."""
        logger.info(f"Extracting zip from {zip_path} to {dest}")

        # If path is a DBFS or UC Volume path, copy locally first
        local_zip_path = zip_path
        if zip_path.startswith("/Volumes/") or zip_path.startswith("dbfs:/"):
            local_zip_path = f"/tmp/{Path(zip_path).name}"
            logger.info(f"Copying from UC Volume to {local_zip_path}")

            try:
                # Try using dbutils (works in notebooks and jobs)
                from pyspark.dbutils import DBUtils
                from pyspark.sql import SparkSession
                spark = SparkSession.builder.getOrCreate()
                dbutils = DBUtils(spark)
                dbutils.fs.cp(zip_path, f"file:{local_zip_path}", recurse=True)
                logger.info(f"Successfully copied via DBUtils")
            except Exception as e1:
                logger.warning(f"DBUtils approach failed: {e1}")
                try:
                    # Fallback: use Spark's hadoop filesystem
                    from pyspark.sql import SparkSession
                    spark = SparkSession.builder.getOrCreate()
                    sc = spark.sparkContext
                    hadoop_conf = sc._jsc.hadoopConfiguration()
                    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
                        sc._jvm.java.net.URI(zip_path), hadoop_conf
                    )
                    src_path = sc._jvm.org.apache.hadoop.fs.Path(zip_path)
                    dst_path = sc._jvm.org.apache.hadoop.fs.Path(f"file:{local_zip_path}")
                    local_fs = sc._jvm.org.apache.hadoop.fs.FileSystem.getLocal(hadoop_conf)
                    sc._jvm.org.apache.hadoop.fs.FileUtil.copy(
                        fs, src_path, local_fs, dst_path, False, hadoop_conf
                    )
                    logger.info(f"Successfully copied via Hadoop FileSystem")
                except Exception as e2:
                    logger.error(f"Hadoop FileSystem approach also failed: {e2}")
                    raise RuntimeError(
                        f"Cannot copy file from UC Volume. DBUtils error: {e1}, "
                        f"Hadoop error: {e2}"
                    )

        try:
            with zipfile.ZipFile(local_zip_path, 'r') as zf:
                zf.extractall(dest)
        except zipfile.BadZipFile as e:
            raise ValueError(f"Invalid or corrupt zip file: {e}")

    def _process_requirements(self, extract_dir: Path, wheel_dir: Path) -> bool:
        """Process requirements.txt if present."""
        req_file = extract_dir / "requirements.txt"
        if not req_file.exists():
            # Check one level deep
            for subdir in extract_dir.iterdir():
                if subdir.is_dir():
                    nested_req = subdir / "requirements.txt"
                    if nested_req.exists():
                        req_file = nested_req
                        break
            else:
                return False

        logger.info(f"Found requirements.txt: {req_file}")

        result = subprocess.run(
            [
                sys.executable, "-m", "pip", "wheel",
                "-r", str(req_file),
                "-w", str(wheel_dir),
                "--platform", self.PLATFORM,
                "--python-version", self.PYTHON_VERSION,
                "--only-binary=:all:",  # Prefer binary wheels
            ],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            # Retry without platform constraints (for pure Python packages)
            logger.warning("Platform-specific wheel download failed, trying without constraints")
            result = subprocess.run(
                [
                    sys.executable, "-m", "pip", "wheel",
                    "-r", str(req_file),
                    "-w", str(wheel_dir),
                ],
                capture_output=True,
                text=True
            )

        if result.returncode != 0:
            logger.error(f"pip wheel failed: {result.stderr}")
            raise RuntimeError(f"Failed to build wheels from requirements.txt: {result.stderr}")

        return True

    def _process_package(self, extract_dir: Path, wheel_dir: Path) -> bool:
        """Process setup.py or pyproject.toml if present."""
        package_dir = extract_dir

        # Check for setup.py or pyproject.toml
        has_setup = (package_dir / "setup.py").exists()
        has_pyproject = (package_dir / "pyproject.toml").exists()

        # Also check one level deep (common zip structure)
        if not (has_setup or has_pyproject):
            for subdir in extract_dir.iterdir():
                if subdir.is_dir():
                    if (subdir / "setup.py").exists() or (subdir / "pyproject.toml").exists():
                        package_dir = subdir
                        has_setup = (package_dir / "setup.py").exists()
                        has_pyproject = (package_dir / "pyproject.toml").exists()
                        break

        if not (has_setup or has_pyproject):
            return False

        logger.info(f"Found Python package in: {package_dir}")

        result = subprocess.run(
            [
                sys.executable, "-m", "pip", "wheel",
                str(package_dir),
                "-w", str(wheel_dir),
                "--no-deps",  # Don't include dependencies, just the package
            ],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            logger.error(f"pip wheel failed: {result.stderr}")
            raise RuntimeError(f"Failed to build wheel from package: {result.stderr}")

        return True

    def _copy_existing_wheels(self, extract_dir: Path, wheel_dir: Path) -> bool:
        """Copy any existing .whl files from the zip."""
        wheels = list(extract_dir.rglob("*.whl"))
        if not wheels:
            return False

        logger.info(f"Found {len(wheels)} existing wheel file(s)")
        for whl in wheels:
            dest = wheel_dir / whl.name
            shutil.copy2(whl, dest)
            logger.info(f"  Copied: {whl.name}")

        return True

    def _process_pure_python(self, extract_dir: Path, wheel_dir: Path) -> bool:
        """
        Handle pure Python modules without packaging.
        Creates a minimal wheel from .py files.
        """
        # Find Python files
        py_files = list(extract_dir.rglob("*.py"))
        if not py_files:
            return False

        # Exclude __pycache__ and test files
        py_files = [
            f for f in py_files
            if "__pycache__" not in str(f) and not f.name.startswith("test_")
        ]

        if not py_files:
            return False

        logger.info(f"Found {len(py_files)} Python file(s), creating minimal wheel")

        # Create a temporary package structure
        with tempfile.TemporaryDirectory() as pkg_tmp:
            pkg_tmp = Path(pkg_tmp)
            pkg_name = "user_modules"
            pkg_dir = pkg_tmp / pkg_name
            pkg_dir.mkdir()

            # Copy all .py files
            for py_file in py_files:
                # Preserve relative structure
                rel_path = py_file.relative_to(extract_dir)
                dest_path = pkg_dir / rel_path
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(py_file, dest_path)

            # Create __init__.py if missing
            if not (pkg_dir / "__init__.py").exists():
                (pkg_dir / "__init__.py").touch()

            # Create minimal pyproject.toml
            pyproject_content = f'''[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "{pkg_name}"
version = "0.1.0"
description = "User-provided Python modules"
requires-python = ">={self.PYTHON_VERSION}"

[tool.setuptools.packages.find]
where = ["."]
'''
            (pkg_tmp / "pyproject.toml").write_text(pyproject_content)

            # Build wheel
            result = subprocess.run(
                [
                    sys.executable, "-m", "pip", "wheel",
                    str(pkg_tmp),
                    "-w", str(wheel_dir),
                    "--no-deps",
                ],
                capture_output=True,
                text=True
            )

            if result.returncode != 0:
                logger.error(f"Failed to build wheel: {result.stderr}")
                return False

        return True

    def _create_wheelhouse(self, wheel_dir: Path, output_name: str = "wheelhouse") -> str:
        """
        Create .wheelhouse.zip from wheel directory.

        Args:
            wheel_dir: Directory containing wheel files
            output_name: Base name for output file (default: "wheelhouse")

        Returns:
            Path to created wheelhouse zip
        """
        wheels = sorted(wheel_dir.glob("*.whl"))

        output_filename = f"{output_name}.zip"
        # Use run_id for path isolation
        final_output_path = f"{self.output_volume_path}/{self.run_id}/{output_filename}"

        # Create locally first, then copy to DBFS/UC Volume if needed
        if self.output_volume_path.startswith("/Volumes/") or self.output_volume_path.startswith("dbfs:/"):
            local_output_path = f"/tmp/{output_filename}"
        else:
            local_output_path = final_output_path

        logger.info(f"Creating wheelhouse: {local_output_path}")

        with zipfile.ZipFile(local_output_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for whl in wheels:
                zf.write(whl, whl.name)

        # Copy to DBFS or UC Volume if needed
        if self.output_volume_path.startswith("/Volumes/") or self.output_volume_path.startswith("dbfs:/"):
            logger.info(f"Copying wheelhouse to UC Volume: {final_output_path}")
            try:
                from pyspark.dbutils import DBUtils
                from pyspark.sql import SparkSession
                spark = SparkSession.builder.getOrCreate()
                dbutils = DBUtils(spark)
                dbutils.fs.cp(f"file:{local_output_path}", final_output_path, recurse=True)
                logger.info(f"Successfully copied wheelhouse via DBUtils")
            except Exception as e1:
                logger.warning(f"DBUtils copy failed: {e1}, trying Hadoop FileSystem")
                try:
                    from pyspark.sql import SparkSession
                    spark = SparkSession.builder.getOrCreate()
                    sc = spark.sparkContext
                    hadoop_conf = sc._jsc.hadoopConfiguration()
                    local_fs = sc._jvm.org.apache.hadoop.fs.FileSystem.getLocal(hadoop_conf)
                    dst_fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
                        sc._jvm.java.net.URI(final_output_path), hadoop_conf
                    )
                    src_path = sc._jvm.org.apache.hadoop.fs.Path(f"file:{local_output_path}")
                    dst_path = sc._jvm.org.apache.hadoop.fs.Path(final_output_path)
                    sc._jvm.org.apache.hadoop.fs.FileUtil.copy(
                        local_fs, src_path, dst_fs, dst_path, False, hadoop_conf
                    )
                    logger.info(f"Successfully copied wheelhouse via Hadoop FileSystem")
                except Exception as e2:
                    logger.error(f"Both copy methods failed: DBUtils={e1}, Hadoop={e2}")
                    raise

        return final_output_path


def process_dependencies(input_zip_path: str, output_volume_path: str, run_id: str = None) -> str:
    """
    Main entry point for processing dependencies.

    Args:
        input_zip_path: Path to user's zip file
        output_volume_path: UC Volume path for output
        run_id: Unique run identifier (generated if not provided)

    Returns:
        Path to created .wheelhouse.zip
    """
    processor = DependencyProcessor(input_zip_path, output_volume_path, run_id)
    return processor.process()


# Databricks notebook/job entry point
if __name__ == "__main__":
    import uuid

    try:
        # Try Databricks widgets first
        from pyspark.sql import SparkSession
        from pyspark.dbutils import DBUtils

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)

        input_path = dbutils.widgets.get("input_zip_path")
        output_volume = dbutils.widgets.get("output_volume_path")

        result_path = process_dependencies(input_path, output_volume)

        # Set task value for downstream task using Python API
        dbutils.jobs.taskValues.set(key="wheelhouse_path", value=result_path)

        print(f"\nSUCCESS: Created wheelhouse at {result_path}")
        print(f"Task value 'wheelhouse_path' set to: {result_path}")

    except Exception as e:
        # Fall back to command-line args (for SparkPythonTask with parameters)
        if len(sys.argv) >= 3:
            from pyspark.sql import SparkSession
            from pyspark.dbutils import DBUtils

            input_path = sys.argv[1]
            output_volume = sys.argv[2]

            # Create processor to get run_id
            processor = DependencyProcessor(input_path, output_volume)
            result_path = processor.process()

            # Set task values for downstream tasks
            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)
            dbutils.jobs.taskValues.set(key="wheelhouse_path", value=result_path)
            dbutils.jobs.taskValues.set(key="run_id", value=processor.run_id)

            print(f"\nSUCCESS: Created wheelhouse at {result_path}")
            print(f"Run ID: {processor.run_id}")
            print(f"Task values set: wheelhouse_path, run_id")
        else:
            print(f"Usage: {sys.argv[0]} <input_zip_path> <output_volume_path>")
            print(f"Error: {e}")
            sys.exit(1)
