"""Fixtures specific to uc_external_tables access method."""

import pytest
from hive_table_experiments import (
    UC_EXTERNAL_HIVE,
    UC_EXTERNAL_DELTA,
    get_scenarios_for_access_method,
)


@pytest.fixture(scope="module")
def uc_ext_config():
    """Access method configuration for Parquet external tables."""
    return UC_EXTERNAL_HIVE


@pytest.fixture(scope="module")
def uc_delta_config():
    """Access method configuration for Delta external tables."""
    return UC_EXTERNAL_DELTA


@pytest.fixture(scope="module")
def uc_ext_scenarios():
    """Scenarios compatible with UC external (Parquet)."""
    return get_scenarios_for_access_method(UC_EXTERNAL_HIVE)


@pytest.fixture(scope="module")
def uc_delta_scenarios():
    """Scenarios compatible with UC external (Delta)."""
    return get_scenarios_for_access_method(UC_EXTERNAL_DELTA)
