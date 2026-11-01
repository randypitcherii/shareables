from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from mlflow.entities.trace_location import UnityCatalog

import driver


@pytest.fixture
def telemetry(monkeypatch):
    location = UnityCatalog(
        catalog_name="catalog",
        schema_name="schema",
        table_prefix="42",
        _otel_spans_table_name="catalog.schema.42_otel_spans",
        _otel_logs_table_name="catalog.schema.42_otel_logs",
        _annotations_table_name="catalog.schema.42_otel_annotations",
    )
    monkeypatch.setattr(driver.mlflow, "get_experiment", lambda experiment_id: SimpleNamespace(trace_location=location))
    workspace = MagicMock()
    monkeypatch.setattr("databricks.sdk.WorkspaceClient", lambda: workspace)
    configured = {
        "telemetry_config": {
            "table_names": {
                "traces_table": location.full_otel_spans_table_name,
                "annotations_table": location.full_annotations_table_name,
            },
            "enabled_telemetry_features": ["TELEMETRY_FEATURE_TRACES"],
        }
    }
    return workspace, configured


def test_configures_uc_tables_and_verifies_readback(telemetry):
    workspace, configured = telemetry
    workspace.api_client.do.side_effect = [{}, {}, configured]

    driver.configure_endpoint_telemetry("endpoint", "42")

    requests = workspace.api_client.do.call_args_list
    assert [request.args[0] for request in requests] == ["GET", "PATCH", "GET"]
    assert requests[1].args[1] == "/api/2.0/serving-endpoints/endpoint/telemetry-config"
    config = requests[1].kwargs["body"]["telemetry_config"]
    assert config["enabled_telemetry_features"] == ["TELEMETRY_FEATURE_TRACES"]
    assert config["table_names"] == {
        "traces_table": "catalog.schema.42_otel_spans",
        "logs_table": "catalog.schema.42_otel_logs",
        "metrics_table": "catalog.schema.42_otel_metrics",
        "annotations_table": "catalog.schema.42_otel_annotations",
    }
    assert workspace.serving_endpoints.wait_get_serving_endpoint_not_updating.call_count == 2


def test_redeploy_does_not_reconfigure_matching_telemetry(telemetry):
    workspace, configured = telemetry
    workspace.api_client.do.return_value = configured

    driver.configure_endpoint_telemetry("endpoint", "42")

    workspace.api_client.do.assert_called_once_with("GET", "/api/2.0/serving-endpoints/endpoint")
    workspace.serving_endpoints.wait_get_serving_endpoint_not_updating.assert_called_once()


def test_accepts_backend_omitting_annotations_table_metadata(telemetry):
    workspace, configured = telemetry
    driver.mlflow.get_experiment("42").trace_location._annotations_table_name = None
    workspace.api_client.do.side_effect = [{}, {}, configured]

    driver.configure_endpoint_telemetry("endpoint", "42")

    config = workspace.api_client.do.call_args_list[1].kwargs["body"]["telemetry_config"]
    assert config["table_names"]["annotations_table"] == "catalog.schema.42_otel_annotations"


def test_replaces_a_previous_experiment_destination(telemetry):
    workspace, configured = telemetry
    previous = {"telemetry_config": {"table_names": {"traces_table": "catalog.schema.old_otel_spans"}}}
    workspace.api_client.do.side_effect = [previous, {}, configured]

    driver.configure_endpoint_telemetry("endpoint", "42")

    assert workspace.api_client.do.call_args_list[1].args[0] == "PATCH"


def test_fails_if_destination_does_not_persist(telemetry):
    workspace, configured = telemetry
    workspace.api_client.do.return_value = {}

    with pytest.raises(RuntimeError, match="did not retain"):
        driver.configure_endpoint_telemetry("endpoint", "42")


def test_refuses_an_unlinked_experiment(monkeypatch):
    monkeypatch.setattr(driver.mlflow, "get_experiment", lambda experiment_id: SimpleNamespace(trace_location=None))
    workspace_factory = MagicMock()
    monkeypatch.setattr("databricks.sdk.WorkspaceClient", workspace_factory)

    with pytest.raises(ValueError, match="not linked"):
        driver.configure_endpoint_telemetry("endpoint", "42")

    workspace_factory.assert_not_called()
