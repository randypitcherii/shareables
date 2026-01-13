"""Fixtures specific to glue_as_hive_metastore access method."""

import pytest
from hive_table_experiments import GLUE_AS_HIVE, get_scenarios_for_access_method


@pytest.fixture(scope="module")
def access_config():
    """Access method configuration for this test module."""
    return GLUE_AS_HIVE


@pytest.fixture(scope="module")
def compatible_scenarios():
    """Scenarios compatible with glue_as_hive_metastore."""
    return get_scenarios_for_access_method(GLUE_AS_HIVE)


@pytest.fixture(scope="class")
def table_name_factory(access_config):
    """Factory for generating table names with _glue_hive suffix."""

    def _make_name(scenario_base: str) -> str:
        return access_config.get_fully_qualified_table(scenario_base)

    return _make_name
