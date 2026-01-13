"""pytest configuration for conversion evaluation tests.

Provides fixtures for testing three Delta conversion methods:
1. CONVERT TO DELTA - In-place conversion adding _delta_log
2. SHALLOW CLONE - Metadata-only reference to existing data
3. Manual Delta Log - Programmatic delta log creation

Tests run against Databricks Connect to evaluate conversion outcomes.
"""

import pytest

from hive_table_experiments import (
    CONVERSION_METHODS,
    get_conversion_matrix,
    get_conversion_source_info,
    list_all_conversion_sources,
)
from hive_table_experiments.scenarios import ALL_SCENARIOS


# Scenario base names
SCENARIO_NAMES = [s.base_name for s in ALL_SCENARIOS]


@pytest.fixture(scope="session")
def conversion_matrix():
    """Get the global conversion matrix for tracking results."""
    return get_conversion_matrix()


@pytest.fixture(scope="session")
def uc_catalog():
    """Unity Catalog catalog name for converted tables."""
    return "your_catalog"


@pytest.fixture(scope="session")
def uc_schema():
    """Unity Catalog schema name for converted tables."""
    import os
    return os.getenv("HIVE_EVAL_UC_SCHEMA", "your_uc_schema")


@pytest.fixture(scope="session")
def glue_database():
    """Glue database name for source tables."""
    import os
    return os.getenv("HIVE_EVAL_GLUE_DATABASE", "your_glue_database")


@pytest.fixture(scope="session")
def conversion_sources():
    """List of all conversion source table info."""
    return list_all_conversion_sources()


@pytest.fixture(scope="session", autouse=True)
def export_conversion_matrix_on_teardown(conversion_matrix):
    """Automatically export conversion matrix after test session."""
    yield
    # Export results after all tests complete
    conversion_matrix.save_markdown("CONVERSION_MATRIX.md")
    conversion_matrix.save_json("conversion_results.json")
    print(f"\n✓ Exported conversion matrix to CONVERSION_MATRIX.md and conversion_results.json")


def get_glue_source_table(scenario: str, method: str, glue_database: str = None) -> str:
    """Get fully-qualified Glue source table name.

    Args:
        scenario: Scenario base name
        method: Conversion method
        glue_database: Glue database name

    Returns:
        Fully-qualified table name via Glue federation catalog
    """
    import os
    if glue_database is None:
        glue_database = os.getenv("HIVE_EVAL_GLUE_DATABASE", "your_glue_database")
    glue_catalog = os.getenv("HIVE_EVAL_GLUE_CATALOG", "your_glue_catalog")
    info = get_conversion_source_info(scenario, method)
    return f"{glue_catalog}.{glue_database}.{info['table_name']}"


def get_uc_target_table(scenario: str, method: str, uc_catalog: str = None, uc_schema: str = None) -> str:
    """Get fully-qualified UC target table name for converted Delta table.

    Args:
        scenario: Scenario base name
        method: Conversion method
        uc_catalog: UC catalog name
        uc_schema: UC schema name

    Returns:
        Fully-qualified UC table name
    """
    import os
    if uc_catalog is None:
        uc_catalog = os.getenv("HIVE_EVAL_UC_CATALOG", "your_catalog")
    if uc_schema is None:
        uc_schema = os.getenv("HIVE_EVAL_UC_SCHEMA", "your_uc_schema")
    return f"{uc_catalog}.{uc_schema}.{scenario}_delta_{method}"
