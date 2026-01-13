"""pytest configuration and fixtures for Hive table experiments.

Provides fixtures for testing three Glue access methods:
1. glue_as_hive_metastore - Spark's built-in Glue integration (hive_metastore catalog)
2. uc_glue_federation - UC foreign catalog pointing to Glue
3. uc_external_tables - Manual UC external tables (Parquet or Delta)
"""

# Load .env file BEFORE importing hive_table_experiments modules
# This ensures environment variables are set before module-level os.getenv() calls
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

import pytest
from databricks.connect import DatabricksSession

from hive_table_experiments import (
    BUCKET_EAST_1A,
    BUCKET_EAST_1B,
    BUCKET_WEST_2,
    GLUE_AS_HIVE,
    UC_GLUE_FEDERATION,
    UC_EXTERNAL_HIVE,
    UC_EXTERNAL_DELTA,
    ALL_ACCESS_METHODS,
    ALL_SCENARIOS,
    ResultsMatrix,
    get_results_matrix,
    reset_results_matrix,
)


# =============================================================================
# Core Spark Session
# =============================================================================


@pytest.fixture(scope="session")
def spark():
    """Create a Databricks Connect session using the Glue-configured cluster.

    This connects to the compute cluster configured with Glue Spark settings
    and allows running Spark commands remotely. The session is created once
    per test session and reused across all tests.

    Returns:
        DatabricksSession: Active Spark session connected to Databricks
    """
    # Use the glue-hive-eval-cluster-v3 (DBR 16.4) configured with Glue access
    session = (
        DatabricksSession.builder.profile("DEFAULT")
        .remote(cluster_id="1231-041629-tyfxt3io")
        .getOrCreate()
    )
    yield session
    session.stop()


# =============================================================================
# Database and Bucket Configuration
# =============================================================================


@pytest.fixture(scope="session")
def glue_database():
    """Return the Glue database name used for test tables."""
    import os
    return os.getenv("HIVE_EVAL_GLUE_DATABASE", "your_glue_database")


@pytest.fixture(scope="session")
def s3_buckets():
    """Return S3 bucket names for test scenarios."""
    return {
        "east_1a": f"s3://{BUCKET_EAST_1A}",
        "east_1b": f"s3://{BUCKET_EAST_1B}",
        "west_2": f"s3://{BUCKET_WEST_2}",
    }


@pytest.fixture(scope="session")
def aws_region():
    """Primary AWS region for Glue catalog."""
    return "us-east-1"


# =============================================================================
# Access Method Configurations
# =============================================================================


@pytest.fixture(scope="session")
def glue_hive_config():
    """Configuration for glue_as_hive_metastore access method."""
    return GLUE_AS_HIVE


@pytest.fixture(scope="session")
def glue_fed_config():
    """Configuration for uc_glue_federation access method."""
    return UC_GLUE_FEDERATION


@pytest.fixture(scope="session")
def uc_ext_config():
    """Configuration for uc_external_tables (Parquet) access method."""
    return UC_EXTERNAL_HIVE


@pytest.fixture(scope="session")
def uc_delta_config():
    """Configuration for uc_external_tables (Delta) access method."""
    return UC_EXTERNAL_DELTA


@pytest.fixture(scope="session")
def all_access_configs():
    """All access method configurations."""
    return ALL_ACCESS_METHODS


# =============================================================================
# Table Name Factories
# =============================================================================


@pytest.fixture(scope="class")
def glue_table(glue_database):
    """Factory for generating fully-qualified table names in hive_metastore.

    Usage:
        def test_something(glue_table):
            table = glue_table("standard_table")
            spark.sql(f"SELECT * FROM {table}")
    """

    def _make_table_name(table_name: str) -> str:
        return f"hive_metastore.{glue_database}.{table_name}"

    return _make_table_name


@pytest.fixture(scope="class")
def table_for_method():
    """Factory for generating fully-qualified table names for any access method.

    Usage:
        def test_something(table_for_method):
            table = table_for_method("standard", "_glue_hive")
            spark.sql(f"SELECT * FROM {table}")
    """
    from hive_table_experiments import get_table_info_for_scenario

    def _make_table_name(scenario_base: str, access_suffix: str) -> str:
        info = get_table_info_for_scenario(scenario_base, access_suffix)
        return info["fully_qualified"]

    return _make_table_name


# =============================================================================
# Results Matrix
# =============================================================================


@pytest.fixture(scope="session")
def results_matrix():
    """Get or create the global results matrix for tracking test outcomes.

    The matrix persists across all tests in the session and can be
    exported to markdown or CSV at the end.
    """
    return get_results_matrix()


@pytest.fixture(scope="session", autouse=True)
def export_results_on_teardown(results_matrix):
    """Automatically export results matrix after test session."""
    yield
    # Export results after all tests complete
    if results_matrix.results:
        results_matrix.save_markdown("RESULTS_MATRIX.md")
        print(f"\n✓ Exported results to RESULTS_MATRIX.md")


# =============================================================================
# Scenario Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def all_scenarios():
    """All test scenarios."""
    return ALL_SCENARIOS


@pytest.fixture(params=ALL_SCENARIOS, ids=lambda s: s.base_name)
def scenario(request):
    """Parametrized fixture yielding each scenario."""
    return request.param


# =============================================================================
# Legacy Compatibility
# =============================================================================


# Keep old fixtures working for existing tests
@pytest.fixture(scope="class")
def glue_hive_table(glue_database):
    """Legacy: Factory for hive_metastore table names (deprecated)."""

    def _make_table_name(table_name: str) -> str:
        return f"hive_metastore.{glue_database}.{table_name}"

    return _make_table_name
