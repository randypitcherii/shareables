from types import SimpleNamespace

from fastapi.testclient import TestClient

import app
from server.routes.metadata import MetadataPrimaryUnavailableError
from server.services import uc_metadata


SAMPLE_ROW = {
    "catalog_name": "main",
    "schema_name": "default",
    "object_name": "orders",
    "object_type": "MANAGED",
    "owner": "data-team",
    "comment": "Orders table",
    "table_tag_count": 1,
    "column_count": 2,
    "columns_missing_description": 0,
    "columns_missing_tags": 0,
    "table_description_missing": False,
    "table_owner_missing": False,
    "table_tags_missing": False,
    "total_gap_score": 0,
}


def test_objects_endpoint_primary_success_uses_primary_headers(monkeypatch):
    monkeypatch.setattr("server.routes.metadata.list_objects_primary", lambda **kwargs: [SAMPLE_ROW])

    client = TestClient(app.app)
    response = client.get("/api/v1/metadata/objects", headers={"Authorization": "Bearer token"})

    assert response.status_code == 200
    assert response.headers.get("x-uc-metadata-mode") == "primary"
    assert response.json()[0]["object_name"] == "orders"


def test_objects_endpoint_primary_failure_is_structured(monkeypatch):
    def _raise_unavailable(**kwargs):
        raise MetadataPrimaryUnavailableError(
            message="Primary metadata loading failed because the SQL warehouse is unavailable or not running.",
            reason_code="warehouse_unavailable",
        )

    monkeypatch.setattr("server.routes.metadata.list_objects_primary", _raise_unavailable)

    client = TestClient(app.app)
    response = client.get("/api/v1/metadata/objects", headers={"Authorization": "Bearer token"})

    assert response.status_code == 503
    assert response.headers.get("x-uc-metadata-primary-failed") == "true"
    assert response.headers.get("x-uc-metadata-primary-reason") == "warehouse_unavailable"
    assert response.headers.get("x-uc-metadata-fallback-available") == "true"


def test_objects_endpoint_fallback_true_returns_fallback_data(monkeypatch):
    fallback_row = {**SAMPLE_ROW, "object_name": "fallback_orders"}
    monkeypatch.setattr("server.routes.metadata.list_objects_fallback", lambda **kwargs: [fallback_row])

    client = TestClient(app.app)
    response = client.get("/api/v1/metadata/objects?fallback=true", headers={"Authorization": "Bearer token"})

    assert response.status_code == 200
    assert response.headers.get("x-uc-metadata-mode") == "fallback"
    assert response.headers.get("x-uc-metadata-fallback-reason") == "user_requested"
    assert response.json()[0]["object_name"] == "fallback_orders"


def test_list_objects_primary_uses_sql_when_queryable(monkeypatch):
    monkeypatch.setattr(uc_metadata, "settings", SimpleNamespace(sql_warehouse_id="warehouse-id"))
    monkeypatch.setattr(uc_metadata, "_warehouse_is_queryable", lambda: True)
    monkeypatch.setattr(uc_metadata, "_list_objects_from_sql", lambda **kwargs: [SAMPLE_ROW])

    result = uc_metadata.list_objects_primary(
        access_token="token",
        search="",
        gap_only=True,
        limit=10,
    )

    assert result[0]["object_name"] == "orders"


def test_list_objects_uses_fallback_when_primary_query_errors(monkeypatch):
    fallback_row = {**SAMPLE_ROW, "object_name": "from_fallback"}
    monkeypatch.setattr(uc_metadata, "settings", SimpleNamespace(sql_warehouse_id="warehouse-id"))
    monkeypatch.setattr(uc_metadata, "_warehouse_is_queryable", lambda: True)

    def _raise_query_error(**kwargs):
        raise RuntimeError("connector failed")

    monkeypatch.setattr(uc_metadata, "_list_objects_from_sql", _raise_query_error)
    monkeypatch.setattr(uc_metadata, "_fallback_list_objects_from_uc_api", lambda **kwargs: [fallback_row])

    result = uc_metadata.list_objects(
        access_token="token",
        search="",
        gap_only=True,
        limit=10,
    )

    assert result[0]["object_name"] == "from_fallback"


def test_warehouse_queryable_accepts_enum_state(monkeypatch):
    class _EnumValue:
        def __init__(self, value):
            self.value = value

    warehouse = SimpleNamespace(
        state=_EnumValue("RUNNING"),
        health=SimpleNamespace(status=_EnumValue("HEALTHY")),
    )

    class _WarehousesApi:
        @staticmethod
        def get(_warehouse_id):
            return warehouse

    class _Client:
        warehouses = _WarehousesApi()

    monkeypatch.setattr(uc_metadata, "settings", SimpleNamespace(sql_warehouse_id="warehouse-id"))
    monkeypatch.setattr(uc_metadata, "get_workspace_client", lambda: _Client())

    assert uc_metadata._warehouse_is_queryable() is True
