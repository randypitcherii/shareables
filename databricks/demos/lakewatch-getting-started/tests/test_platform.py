from types import SimpleNamespace

import pytest
from databricks.sdk.errors import NotFound, PermissionDenied

from rpw_lakewatch.errors import SetupError
from rpw_lakewatch.platform import Platform


class Api:
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def do(self, method, path, query=None, body=None):
        self.calls.append((method, path, body))
        r = self.routes.get((method, path))
        if isinstance(r, Exception):
            raise r
        return r() if callable(r) else r


class Schemas:
    def __init__(self, exists=False):
        self.exists, self.created = exists, []

    def create(self, name, catalog_name, comment=None):
        if self.exists:
            from databricks.sdk.errors import ResourceAlreadyExists
            raise ResourceAlreadyExists("Schema already exists")
        self.created.append(f"{catalog_name}.{name}")


def platform(routes=None, catalogs=None, schemas=None):
    w = SimpleNamespace(api_client=Api(routes or {}), catalogs=catalogs, schemas=schemas or Schemas())
    return Platform(w)


def test_prior_preview_detected():
    p = platform({("GET", "/api/2.0/dasl-apiserver/apis/workspace/v1/config"): {"kind": "WorkspaceConfig"}})
    assert p.prior_preview_setup() is True


def test_prior_preview_absent_on_404():
    p = platform({("GET", "/api/2.0/dasl-apiserver/apis/workspace/v1/config"): NotFound("No API found for 'GET /x'")})
    assert p.prior_preview_setup() is False


def test_environments_unavailable_is_none():
    p = platform({("GET", "/api/2.0/lakewatch/environments"): PermissionDenied("FEATURE_DISABLED")})
    assert p.environments() is None


def test_environments_listed():
    p = platform({("GET", "/api/2.0/lakewatch/environments"): {"environments": [{"catalog_name": "a"}]}})
    assert p.environments() == ["a"]


class Catalogs:
    def __init__(self, exists=False, create_error=None):
        self.exists, self.create_error, self.created = exists, create_error, []

    def get(self, name):
        if not self.exists:
            raise NotFound("no")

    def create(self, name, comment=None):
        if self.create_error:
            raise self.create_error
        self.created.append(name)


def test_catalog_reused_when_present():
    c = Catalogs(exists=True)
    assert platform(catalogs=c).ensure_catalog("x", {}) is False and not c.created


def test_catalog_created_on_default_storage():
    c = Catalogs()
    assert platform(catalogs=c).ensure_catalog("x", {}) is True and c.created == ["x"]


def test_no_default_storage_maps_to_e201():
    c = Catalogs(create_error=Exception("Metastore storage root URL does not exist. Please provide a storage location"))
    with pytest.raises(SetupError) as e:
        platform(catalogs=c).ensure_catalog("x", {})
    assert e.value.code == "LWGS-E201" and "--storage-root" in e.value.render() and "Default Storage" in e.value.render()


def test_permission_maps_to_e202():
    c = Catalogs(create_error=PermissionDenied("User does not have CREATE CATALOG on Metastore"))
    with pytest.raises(SetupError) as e:
        platform(catalogs=c).ensure_catalog("x", {})
    assert e.value.code == "LWGS-E202"


def test_folder_idempotent():
    p = platform({("GET", "/api/2.0/lakewatch/folders"): {"folders": [{"schema_name": "rpw_databricks_security_infra"}]}})
    assert p.ensure_folder("c", "rpw_databricks_security_infra", {}) is False
    assert all(m == "GET" for m, _, _ in p.w.api_client.calls)


def test_folder_creates_schema_then_registers():
    sch = Schemas()
    p = platform({("GET", "/api/2.0/lakewatch/folders"): {"folders": []},
                  ("POST", "/api/2.0/lakewatch/folders"): {}}, schemas=sch)
    assert p.ensure_folder("c", "rpw_databricks_security_infra", {}) is True
    assert sch.created == ["c.rpw_databricks_security_infra"]
    assert ("POST", "/api/2.0/lakewatch/folders", {"catalog_name": "c", "schema_name": "rpw_databricks_security_infra"}) in p.w.api_client.calls


def test_folder_registers_existing_schema():
    p = platform({("GET", "/api/2.0/lakewatch/folders"): {"folders": []},
                  ("POST", "/api/2.0/lakewatch/folders"): {}}, schemas=Schemas(exists=True))
    assert p.ensure_folder("c", "rpw_databricks_security_infra", {}) is True


def test_register_environment_initializes_metastore_then_retries(monkeypatch):
    """Fresh workspaces: 'parent of environment does not exist' until the metastore is initialized."""
    from databricks.sdk.errors import InvalidParameterValue
    monkeypatch.setattr("rpw_lakewatch.platform.time.sleep", lambda s: None)
    state = {"init": False}

    def env():
        if not state["init"]:
            raise InvalidParameterValue("parent of environment does not exist")
        return {}

    def init():
        state["init"] = True
        return {"state": "SUCCEEDED"}

    p = platform({("POST", "/api/2.0/lakewatch/environments"): env,
                  ("POST", "/api/2.0/lakewatch/metastore:ensureInitialized"): init})
    p.register_environment("c", "wh", {})
    paths = [c[1] for c in p.w.api_client.calls]
    assert paths == ["/api/2.0/lakewatch/environments", "/api/2.0/lakewatch/metastore:ensureInitialized",
                     "/api/2.0/lakewatch/environments"]


def test_register_environment_not_ready_explains(monkeypatch):
    from databricks.sdk.errors import InvalidParameterValue
    monkeypatch.setattr("rpw_lakewatch.platform.time.sleep", lambda s: None)
    p = platform({("POST", "/api/2.0/lakewatch/environments"): InvalidParameterValue("parent of environment does not exist"),
                  ("POST", "/api/2.0/lakewatch/metastore:ensureInitialized"): {"state": "PENDING"}})
    with pytest.raises(SetupError) as ei:
        p.register_environment("c", "wh", {}, timeout_s=0)
    assert ei.value.code == "LWGS-E203" and "not initialized" in ei.value.likely_cause
