"""
Job definition for testing packaged format (pyproject.toml/setup.py).

Tests Python packages with proper build configuration.
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


def create_packaged_test_job(
    job_name: str = "[dev cluster] packaged-test",
    volume_base_path: str = DEFAULT_VOLUME_BASE,
    cleanup_enabled: bool = DEFAULT_CLEANUP_ENABLED,
) -> Job:
    """
    Create job for testing packaged format (pyproject.toml/setup.py).

    Args:
        job_name: Name for the job
        volume_base_path: Base path for storing artifacts
        cleanup_enabled: Whether to cleanup after job completion

    Returns:
        Job definition
    """
    # Paths
    input_zip_path = f"{volume_base_path}/input/packaged.zip"
    output_dir = volume_base_path

    # Bundle files (deployed by DABs)
    processor_script = "file:${workspace.file_path}/src/dependency_processor.py"
    test_script = "file:${workspace.file_path}/src/test_packaged.py"
    cleanup_script = "file:${workspace.file_path}/src/cleanup.py"

    return Job(
        name=job_name,
        tasks=[
            # Task 1: Process dependencies on 2-node cluster
            Task(
                task_key="process_deps",
                description="Build packaged.zip into wheelhouse format",
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
            # Task 2: Test packaged modules on driver + workers
            Task(
                task_key="test_dependencies",
                description="Verify packaged modules work on driver and workers",
                depends_on=[TaskDependency(task_key="process_deps")],
                spark_python_task=SparkPythonTask(
                    python_file=test_script,
                ),
                job_cluster_key="shared_cluster",  # Reuse same cluster
                timeout_seconds=1800,
            ),
            # Task 3: Cleanup on shared cluster
            Task(
                task_key="cleanup",
                description="Clean up temporary artifacts",
                depends_on=[
                    TaskDependency(task_key="test_dependencies"),
                ],
                run_if=RunIf.NONE_FAILED,
                spark_python_task=SparkPythonTask(
                    python_file=cleanup_script,
                    parameters=[
                        volume_base_path,
                        "--cleanup-enabled",
                        str(cleanup_enabled).lower(),
                    ],
                ),
                job_cluster_key="shared_cluster",
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
            # 2-node cluster (1 driver + 1 worker) shared across all tasks
            JobCluster(
                job_cluster_key="shared_cluster",
                new_cluster=ClusterSpec(
                    spark_version="14.3.x-scala2.12",
                    num_workers=1,
                    node_type_id="i3.xlarge",
                    data_security_mode=DataSecurityMode.SINGLE_USER,
                    custom_tags={
                        "ResourceClass": "JobCluster",
                        "Purpose": "PackagedTest",
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
        max_concurrent_runs=5,
        tags={
            "framework": "dynamic-deps",
            "compute_type": "job_cluster",
            "test": "packaged",
        },
    )


# Export for PyDABs discovery
job_packaged_test = create_packaged_test_job()
