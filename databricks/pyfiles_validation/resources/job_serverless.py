"""
Job definition using serverless compute for all tasks.

Three-task job pattern:
  Task 1: Process user's zip -> wheelhouse.zip (serverless)
  Task 2: Test dependencies on driver and workers (serverless)
  Task 3: Cleanup run directory (serverless)
"""
from databricks.bundles.jobs import (
    Job,
    Task,
    TaskDependency,
    SparkPythonTask,
    Library,
    PythonPyPiLibrary,
    RunIf,
)

# Default configuration
DEFAULT_VOLUME_BASE = "dbfs:/FileStore/pyfiles_test"
DEFAULT_CLEANUP_ENABLED = True


def create_job_serverless_variant(
    job_name: str = "[dev serverless] dynamic-deps-test",
    user_zip_filename: str = "requirements_only.zip",
    volume_base_path: str = DEFAULT_VOLUME_BASE,
    cleanup_enabled: bool = DEFAULT_CLEANUP_ENABLED,
) -> Job:
    """
    Create job with serverless compute for all tasks.

    All three tasks use serverless for instant startup and auto-scaling.

    Args:
        job_name: Name for the job
        user_zip_filename: Name of user's zip file in test_artifacts/
        volume_base_path: Base path for storing artifacts
        cleanup_enabled: Whether to cleanup after job completion

    Returns:
        Job definition
    """
    # Filename-based isolation for concurrent processing
    # Using zip filename instead of run_id since libraries param requires static paths
    zip_basename = user_zip_filename.replace('.zip', '')
    input_zip_path = f"{volume_base_path}/input/{user_zip_filename}"
    output_dir = f"{volume_base_path}/output"
    wheelhouse_path = f"{output_dir}/{zip_basename}.wheelhouse.zip"

    # Cleanup will remove both input and output for this zip
    cleanup_pattern = zip_basename

    # Bundle files (deployed by DABs)
    processor_script = "file:${workspace.file_path}/src/dependency_processor.py"
    test_script = "file:${workspace.file_path}/src/test_dependencies.py"
    cleanup_script = "file:${workspace.file_path}/src/cleanup.py"

    return Job(
        name=job_name,
        tasks=[
            # Task 1: Process dependencies on serverless
            Task(
                task_key="process_deps",
                description="Convert user's zip file to wheelhouse format",
                spark_python_task=SparkPythonTask(
                    python_file=processor_script,
                    parameters=[input_zip_path, output_dir],
                ),
                environment_key="default",
                libraries=[
                    Library(pypi=PythonPyPiLibrary(package="wheel")),
                    Library(pypi=PythonPyPiLibrary(package="setuptools>=61.0")),
                ],
                timeout_seconds=1800,
            ),
            # Task 2: Test dependencies on serverless
            Task(
                task_key="test_dependencies",
                description="Verify dependencies work on driver and workers",
                depends_on=[TaskDependency(task_key="process_deps")],
                spark_python_task=SparkPythonTask(
                    python_file=test_script,
                ),
                environment_key="default",
                libraries=[
                    Library(whl=wheelhouse_path),
                ],
                timeout_seconds=1800,
            ),
            # Task 3: Cleanup on serverless (runs even on failure)
            Task(
                task_key="cleanup",
                description="Clean up temporary artifacts",
                depends_on=[
                    TaskDependency(task_key="test_dependencies"),
                ],
                run_if=RunIf.NONE_FAILED,  # Runs unless upstream was cancelled/skipped
                spark_python_task=SparkPythonTask(
                    python_file=cleanup_script,
                    parameters=[
                        volume_base_path,
                        cleanup_pattern,
                        "--cleanup-enabled",
                        str(cleanup_enabled).lower(),
                    ],
                ),
                environment_key="default",
                timeout_seconds=600,
            ),
        ],
        environments=[
            {
                "environment_key": "default",
                "spec": {
                    "client": "1",
                },
            },
        ],
        parameters=[
            {
                "name": "volume_base_path",
                "default": DEFAULT_VOLUME_BASE,
            },
            {
                "name": "cleanup_enabled",
                "default": str(DEFAULT_CLEANUP_ENABLED).lower(),
            },
        ],
        timeout_seconds=7200,
        max_concurrent_runs=10,  # Support concurrent test runs
        tags={
            "framework": "dynamic-deps",
            "compute_type": "serverless",
            "test": "e2e",
        },
    )


# Export for PyDABs discovery
job_serverless_variant = create_job_serverless_variant()
