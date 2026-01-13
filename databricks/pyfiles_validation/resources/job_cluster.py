"""
Job definition using 2-node job cluster (1 driver + 1 worker).

Three-task job pattern:
  Task 1: Process user's zip -> wheelhouse.zip
  Task 2: Test dependencies on driver and workers
  Task 3: Cleanup run directory (serverless)
"""
from databricks.bundles.jobs import (
    Job,
    Task,
    TaskDependency,
    JobCluster,
    ClusterSpec,
    SparkPythonTask,
    Library,
    PythonPyPiLibrary,
    DataSecurityMode,
    RunIf,
)

# Default configuration
DEFAULT_VOLUME_BASE = "dbfs:/FileStore/pyfiles_test"
DEFAULT_CLEANUP_ENABLED = True


def create_job_cluster_variant(
    job_name: str = "[dev cluster] dynamic-deps-test",
    user_zip_filename: str = "requirements_only.zip",
    volume_base_path: str = DEFAULT_VOLUME_BASE,
    cleanup_enabled: bool = DEFAULT_CLEANUP_ENABLED,
) -> Job:
    """
    Create job with 2-node cluster for processing and testing.

    Cluster is shared between Task 1 and Task 2 to avoid double cold start.
    Cleanup task uses serverless for instant execution.

    Args:
        job_name: Name for the job
        user_zip_filename: Name of user's zip file in test_artifacts/
        volume_base_path: Base path for storing artifacts
        cleanup_enabled: Whether to cleanup after job completion

    Returns:
        Job definition
    """
    # Paths
    input_zip_path = f"{volume_base_path}/input/{user_zip_filename}"
    output_dir = volume_base_path

    # Bundle files (deployed by DABs)
    processor_script = "file:${workspace.file_path}/src/dependency_processor.py"
    test_script = "file:${workspace.file_path}/src/test_dependencies.py"
    cleanup_script = "file:${workspace.file_path}/src/cleanup.py"

    return Job(
        name=job_name,
        tasks=[
            # Task 1: Process dependencies on 2-node cluster
            Task(
                task_key="process_deps",
                description="Convert user's zip file to wheelhouse format",
                spark_python_task=SparkPythonTask(
                    python_file=processor_script,
                    parameters=[input_zip_path, output_dir],
                ),
                job_cluster_key="shared_cluster",
                libraries=[
                    Library(pypi=PythonPyPiLibrary(package="wheel")),
                    Library(pypi=PythonPyPiLibrary(package="setuptools>=61.0")),
                ],
                timeout_seconds=1800,
            ),
            # Task 2: Test dependencies on 2-node cluster (driver + 1 worker)
            # Note: Dependencies loaded dynamically via addPyFile() instead of libraries param
            Task(
                task_key="test_dependencies",
                description="Verify dependencies work on driver and workers",
                depends_on=[TaskDependency(task_key="process_deps")],
                spark_python_task=SparkPythonTask(
                    python_file=test_script,
                ),
                job_cluster_key="shared_cluster",  # Reuse same cluster
                # No libraries parameter - wheelhouse loaded at runtime via addPyFile()
                timeout_seconds=1800,
            ),
            # Task 3: Cleanup on shared cluster (runs even on failure)
            # TODO: Move to serverless after validating basic flow works
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
                        "--cleanup-enabled",
                        str(cleanup_enabled).lower(),
                    ],
                ),
                job_cluster_key="shared_cluster",  # Temporary: use same cluster
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
        job_clusters=[
            # 2-node cluster (1 driver + 1 worker) shared across Tasks 1 & 2
            JobCluster(
                job_cluster_key="shared_cluster",
                new_cluster=ClusterSpec(
                    spark_version="14.3.x-scala2.12",
                    num_workers=1,  # 1 worker node
                    node_type_id="i3.xlarge",
                    data_security_mode=DataSecurityMode.SINGLE_USER,
                    custom_tags={
                        "ResourceClass": "JobCluster",
                        "Purpose": "DynamicDeps",
                    },
                ),
            ),
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
            "compute_type": "job_cluster",
            "test": "e2e",
        },
    )


# Export for PyDABs discovery
# Currently validated with requirements_only.zip
# Other formats need format-specific test dependencies
job_cluster_variant = create_job_cluster_variant(
    user_zip_filename="requirements_only.zip"
)
