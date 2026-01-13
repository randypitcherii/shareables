"""Fixtures specific to uc_glue_federation access method."""

import pytest
from hive_table_experiments import UC_GLUE_FEDERATION, get_scenarios_for_access_method


@pytest.fixture(scope="module")
def access_config():
    """Access method configuration for this test module."""
    return UC_GLUE_FEDERATION


@pytest.fixture(scope="module")
def compatible_scenarios():
    """Scenarios compatible with uc_glue_federation."""
    return get_scenarios_for_access_method(UC_GLUE_FEDERATION)


@pytest.fixture(scope="class")
def table_name_factory(access_config):
    """Factory for generating table names with _glue_fed suffix."""

    def _make_name(scenario_base: str) -> str:
        return access_config.get_fully_qualified_table(scenario_base)

    return _make_name
