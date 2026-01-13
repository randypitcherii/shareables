"""
PyDABs resource definitions for dynamic Python dependencies validation.

Exports:
    - job_cluster_variant: 2-node cluster for Tasks 1-2, serverless for cleanup
    - job_serverless_variant: Serverless compute for all tasks
"""
from databricks.bundles.core import Resources

from .job_cluster import job_cluster_variant
from .job_pure_python import job_pure_python_test
from .job_packaged import job_packaged_test
from .job_serverless import job_serverless_variant


def load_resources() -> Resources:
    """
    Load all job resources for PyDABs discovery.

    Returns:
        Resources object with all job definitions
    """
    resources = Resources()
    resources.add_job("dynamic_deps_cluster", job_cluster_variant)
    resources.add_job("test_pure_python", job_pure_python_test)
    resources.add_job("test_packaged", job_packaged_test)
    # Serverless variant temporarily disabled due to libraries parameter limitations
    # resources.add_job("dynamic_deps_serverless", job_serverless_variant)
    return resources
