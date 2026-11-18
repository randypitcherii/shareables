"""Thin wrappers over the Databricks APIs the setup needs, mapped to SetupErrors.

Lakewatch Beta has no Declarative Automation Bundle resource types yet, so
environment, folder and rule management goes through the public REST API. Each
call is idempotent: read first, create only what is missing.
"""
from __future__ import annotations

import json
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound, PermissionDenied, ResourceAlreadyExists
from databricks.sdk.service.settingsv2 import BooleanMessage, Setting

from .errors import SetupError

SETTING = "lakewatch"  # Settings > Previews > "Lakewatch" (Beta)
LEGACY_CONFIG = "/api/2.0/dasl-apiserver/apis/workspace/v1/config"
_ABSENT_MARKERS = ("not found", "no api found", "does not exist", "feature_disabled", "not enabled", "not_implemented")


def _storage_example(cloud: str | None) -> str:
    return {"azure": "abfss://CONTAINER@STORAGE_ACCOUNT.dfs.core.windows.net/rpw_lakewatch",
            "gcp": "gs://YOUR_BUCKET/rpw_lakewatch"}.get(cloud or "", "s3://YOUR_BUCKET/rpw_lakewatch")


def _e201_steps(catalog: str, example: str, have_locations: bool, default_storage: bool) -> list[str]:
    ui = (f"Create the catalog '{catalog}' yourself in Catalog Explorer (+ > Create a catalog; pick Default Storage "
          "if it is offered), then re-run the job; it reuses the catalog.")
    loc = (("Pick a storage location under one of the external locations listed in the facts below (or run: "
            "databricks external-locations list), then redeploy with it and re-run: " if have_locations else
            "Find a storage location you can use (databricks external-locations list; a metastore admin can create "
            "one), then redeploy with it and re-run: ")
           + f"databricks bundle deploy --var catalog_storage_root='{example}' && "
           "databricks bundle run rpw_lakewatch_getting_started")
    steps = [ui, loc] if default_storage else [loc, ui]
    return [("Easiest: " if default_storage else "") + steps[0], "Or: " + steps[1],
            "Or ask a metastore admin to set a metastore storage root."]


def _absent(e: Exception) -> bool:
    return isinstance(e, NotFound) or any(m in str(e).lower() for m in _ABSENT_MARKERS)


class Platform:
    def __init__(self, w: WorkspaceClient):
        self.w = w

    # ---------- facts ----------
    def facts(self) -> dict:
        f = {"workspace_id": None, "host": self.w.config.host, "cloud": None, "metastore": None, "region": None}
        try:
            f["workspace_id"] = str(self.w.get_workspace_id())
        except Exception as e:  # noqa: BLE001 - best effort context only
            f["workspace_id_error"] = str(e)[:200]
        cfg = self.w.config
        f["cloud"] = "azure" if cfg.is_azure else "gcp" if cfg.is_gcp else "aws"
        return f

    def require_admin(self, ctx: dict) -> str:
        me = self.w.current_user.me()
        groups = {g.display for g in (me.groups or [])}
        ctx["run_as"] = me.user_name
        if "admins" not in groups:
            raise SetupError(
                "LWGS-E001",
                what_happened=f"The job runs as '{me.user_name}', which is not in this workspace's 'admins' group.",
                likely_cause="Turning on a preview and creating a Lakewatch environment need workspace admin. "
                             "Creating a catalog also needs CREATE CATALOG on the metastore.",
                next_steps=["Deploy and run the bundle as a workspace admin (dev target runs as the deployer), or",
                            "set `run_as` in databricks.yml to an admin service principal and redeploy."],
                context=ctx)
        return me.user_name

    def require_unity_catalog(self, ctx: dict) -> dict:
        try:
            m = self.w.metastores.summary()
        except Exception as e:  # noqa: BLE001
            raise SetupError(
                "LWGS-E002",
                what_happened=f"Reading the workspace's Unity Catalog metastore failed: {str(e)[:300]}",
                likely_cause="Lakewatch requires Unity Catalog; this workspace has no metastore assigned.",
                next_steps=["Ask an account admin to assign a Unity Catalog metastore to this workspace "
                            "(Account console > Catalog).", "Re-run the job."],
                context=ctx) from e
        ctx.update(metastore=m.name, region=m.region, metastore_storage_root=bool(m.storage_root))
        return {"name": m.name, "storage_root": m.storage_root, "region": m.region}

    # ---------- preview toggle ----------
    def beta_setting(self) -> bool | None:
        try:
            s = self.w.workspace_settings_v2.get_public_workspace_setting(SETTING)
        except Exception:  # noqa: BLE001 - unknown is a valid answer
            return None
        v = s.effective_boolean_val or s.boolean_val
        return None if v is None else bool(v.value)

    def enable_beta(self, ctx: dict) -> None:
        try:
            self.w.workspace_settings_v2.patch_public_workspace_setting(
                SETTING, Setting(name=SETTING, boolean_val=BooleanMessage(value=True)))
        except Exception as e:  # noqa: BLE001
            raise SetupError(
                "LWGS-E102",
                what_happened=f"Turning on the workspace preview '{SETTING}' failed: {str(e)[:300]}",
                likely_cause="The Lakewatch Beta is not offered for this workspace's cloud, region or pricing tier, "
                             "or the caller lacks workspace admin.",
                next_steps=["Open Settings > Previews in the workspace and look for 'Lakewatch'. If it is not listed, "
                            "the Beta is not available here yet; contact your Databricks account team.",
                            "If it is listed, turn it on there and re-run the job."],
                context=ctx) from e

    # ---------- Lakewatch environment ----------
    def environments(self) -> list[str] | None:
        try:
            r = self.w.api_client.do("GET", "/api/2.0/lakewatch/environments", query={"page_size": 100})
        except Exception as e:  # noqa: BLE001
            if _absent(e) or isinstance(e, PermissionDenied):
                return None
            raise
        return [e.get("catalog_name") for e in (r or {}).get("environments", [])]

    def wait_for_environments_api(self, ctx: dict, timeout_s: int = 300) -> list[str]:
        deadline = time.time() + timeout_s
        while True:
            envs = self.environments()
            if envs is not None:
                return envs
            if time.time() > deadline:
                raise SetupError(
                    "LWGS-E103",
                    what_happened=f"The Lakewatch Beta preview is on, but GET /api/2.0/lakewatch/environments was "
                                  f"still unavailable after {timeout_s}s.",
                    likely_cause="A newly turned-on preview can take a few minutes to take effect, or Lakewatch is "
                                 "not available in this region yet.",
                    next_steps=["Wait 10 minutes and re-run the job.",
                                "Open Lakewatch from the app switcher (top-left grid icon). If it is missing, check "
                                "Settings > Previews."],
                    context=ctx)
            time.sleep(15)

    def prior_preview_setup(self) -> bool | None:
        try:
            r = self.w.api_client.do("GET", LEGACY_CONFIG)
        except Exception as e:  # noqa: BLE001
            return False if _absent(e) else None
        return bool(r and r.get("kind") == "WorkspaceConfig")

    def external_locations(self, limit: int = 5) -> list[dict]:
        """External locations the run-as identity can see (name + url), for E201 guidance. Best effort."""
        try:
            locs = (x for x in self.w.external_locations.list() if not (x.name or "").startswith("__databricks"))
            return [{"name": x.name, "url": x.url} for _, x in zip(range(limit), locs)]
        except Exception:  # noqa: BLE001
            return []

    def ensure_catalog(self, catalog: str, ctx: dict, storage_root: str | None = None) -> bool:
        """Create the catalog (on the metastore's default storage, or at storage_root). Returns True if created."""
        try:
            self.w.catalogs.get(catalog)
            return False
        except NotFound:
            pass
        try:
            self.w.catalogs.create(name=catalog, storage_root=storage_root,
                                   comment="Lakewatch environment (lakewatch-getting-started bundle)")
            return True
        except ResourceAlreadyExists:
            return False
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if not storage_root and any(k in msg.lower() for k in ("storage root", "managed location", "default storage", "storage location")):
                locs = self.external_locations()
                example = (locs[0]["url"].rstrip("/") + f"/{catalog}") if locs else _storage_example(ctx.get("cloud"))
                ctx = {**ctx, "external_locations": locs or "none visible to the run-as identity"}
                raise SetupError(
                    "LWGS-E201",
                    what_happened=f"CREATE CATALOG {catalog} failed: {msg[:400]}",
                    likely_cause="The metastore has no storage root, so a new catalog needs an explicit storage "
                                 "location. Lakewatch has not been turned on yet; nothing was changed.",
                    next_steps=_e201_steps(catalog, example, bool(locs), "default storage is enabled" in msg.lower()),
                    context=ctx) from e
            if storage_root:
                raise SetupError(
                    "LWGS-E202",
                    what_happened=f"CREATE CATALOG {catalog} at {storage_root} failed: {msg[:400]}",
                    likely_cause="The storage location is not under an external location the run-as identity can "
                                 "use (it needs CREATE MANAGED STORAGE on it), or the URL is mistyped.",
                    next_steps=["Run databricks external-locations list and pick a URL under one of them.",
                                "Grant: GRANT CREATE MANAGED STORAGE ON EXTERNAL LOCATION `LOCATION_NAME` TO "
                                "`RUN_AS_IDENTITY`, then redeploy with --var catalog_storage_root=... and re-run.",
                                f"Or create the catalog '{catalog}' in Catalog Explorer (+ > Create a catalog; pick "
                                "Default Storage if offered), then redeploy with --var catalog_storage_root='' and "
                                "re-run; the job reuses the catalog."],
                    context=ctx) from e
            raise SetupError(
                "LWGS-E202",
                what_happened=f"CREATE CATALOG {catalog} failed: {msg[:300]}",
                likely_cause="The run-as identity lacks CREATE CATALOG on the metastore.",
                next_steps=["Grant it: GRANT CREATE CATALOG ON METASTORE TO `RUN_AS_USER_OR_SP` (metastore admin).",
                            f"Or create the catalog '{catalog}' yourself and grant the run-as identity MANAGE and "
                            "CREATE SCHEMA on it, then re-run."],
                context=ctx) from e

    def _init_metastore(self, catalog: str | None, warehouse_id: str) -> str | None:
        """Best-effort metastore-level Lakewatch initialization. Returns the reported state, or None on error."""
        body = {"warehouse_id": warehouse_id, **({"catalog": catalog} if catalog else {})}
        try:
            return (self.w.api_client.do("POST", "/api/2.0/lakewatch/metastore:ensureInitialized", body=body) or {}).get("state")
        except DatabricksError:
            return None

    def register_environment(self, catalog: str, warehouse_id: str, ctx: dict, timeout_s: int = 600) -> None:
        """Register the catalog as the Lakewatch environment.

        On a workspace where Lakewatch was never used, registration can fail with "parent of environment does not
        exist" until the metastore is initialized for Lakewatch and the Beta toggle has propagated (minutes). So on
        that error: initialize the metastore, then retry with backoff.
        """
        deadline, delay, err = time.time() + timeout_s, 10, None
        while True:
            try:
                self.w.api_client.do("POST", "/api/2.0/lakewatch/environments", body={"catalog_name": catalog})
                return
            except ResourceAlreadyExists:
                return
            except DatabricksError as e:
                err = e
                msg = str(e).lower()
                if "already exists" in msg:
                    return
                if "parent of environment does not exist" not in msg or time.time() > deadline:
                    break
                state = self._init_metastore(None, warehouse_id)
                if state != "SUCCEEDED":
                    state = self._init_metastore(catalog, warehouse_id)
                print(json.dumps({"step": "environment", "retry": "environment parent not ready; initialized "
                                  "the metastore for Lakewatch", "init_state": state, "next_try_s": delay}), flush=True)
                time.sleep(delay)
                delay = min(delay * 2, 60)
        not_ready = "parent of environment does not exist" in str(err).lower()
        raise SetupError(
            "LWGS-E203",
            what_happened=f"Registering catalog '{catalog}' as a Lakewatch environment failed: {str(err)[:300]}",
            likely_cause=("Lakewatch is not initialized on this metastore yet. On a workspace where Lakewatch was "
                          "never used, this clears within about 10 minutes of turning on the Beta; the job retried "
                          f"for {timeout_s // 60} minutes." if not_ready else
                          "The run-as identity needs MANAGE and CREATE SCHEMA on the catalog, or the catalog "
                          "uses the legacy Hive metastore."),
            next_steps=(["Wait 10 minutes and re-run the job. It picks up where it left off.",
                         "If it still fails, initialize Lakewatch on the metastore, then re-run: databricks api post "
                         "/api/2.0/lakewatch/metastore:ensureInitialized --json "
                         f"'{{\"warehouse_id\": \"{warehouse_id}\"}}'",
                         "Avoid the Lakewatch UI setup here: it creates its own `lakewatch` catalog, which leaves "
                         "a second, unused environment."] if not_ready else
                        ["Confirm the catalog is a Unity Catalog managed catalog you own.",
                         "Alternatively run the Lakewatch setup once in the UI (app switcher > Lakewatch) "
                         "choosing this catalog, then re-run the job; it will reuse it."]),
            context=ctx) from err

    def ensure_initialized(self, catalog: str, warehouse_id: str, ctx: dict, timeout_s: int = 600) -> None:
        deadline = time.time() + timeout_s
        last = None
        while time.time() < deadline:
            try:
                last = self.w.api_client.do("POST", "/api/2.0/lakewatch/metastore:ensureInitialized",
                                            body={"catalog": catalog, "warehouse_id": warehouse_id})
            except DatabricksError as e:
                last = {"state": "ERROR", "message": str(e)[:300]}
                break
            if (last or {}).get("state") == "SUCCEEDED":
                return
            if (last or {}).get("state") in ("FAILED", "ERROR"):
                break
            time.sleep(10)
        raise SetupError(
            "LWGS-E204",
            what_happened=f"Lakewatch initialization for '{catalog}' did not succeed: {last}",
            likely_cause="The SQL warehouse is stopped or below the minimum version, or the run-as identity "
                         "cannot use it.",
            next_steps=[f"Check that warehouse {warehouse_id} is serverless, running, and the run-as identity has "
                        "CAN USE on it.", "Re-run the job."],
            context=ctx)

    def folders(self, catalog: str) -> list[str]:
        r = self.w.api_client.do("GET", "/api/2.0/lakewatch/folders", query={"catalog_name": catalog, "page_size": 100})
        return [f.get("schema_name") for f in (r or {}).get("folders", [])]

    def ensure_folder(self, catalog: str, schema: str, ctx: dict) -> bool:
        """A Lakewatch folder is a Unity Catalog schema registered with Lakewatch. Returns True if registered now."""
        err = None
        try:
            if schema in self.folders(catalog):
                return False
            # The folders API registers an existing schema, so create it first (no-op if present).
            try:
                self.w.schemas.create(name=schema, catalog_name=catalog,
                                      comment="Databricks security infrastructure detections and findings (Lakewatch getting started)")
            except ResourceAlreadyExists:
                pass
            except DatabricksError as e:
                if "already exists" not in str(e).lower():
                    raise
            self.w.api_client.do("POST", "/api/2.0/lakewatch/folders", body={"catalog_name": catalog, "schema_name": schema})
            return True
        except DatabricksError as e:
            err = e
            try:
                if schema in self.folders(catalog):
                    return False
            except DatabricksError:
                pass
        if err is not None:
            raise SetupError(
                "LWGS-E205",
                what_happened=f"Creating Lakewatch folder {catalog}.{schema} failed: {str(err)[:300]}",
                likely_cause="The run-as identity lacks CREATE SCHEMA on the catalog.",
                next_steps=[f"GRANT CREATE SCHEMA, USE CATALOG ON CATALOG {catalog} TO `RUN_AS_IDENTITY`.",
                            f"If a schema named {schema} already exists and is not a Lakewatch folder, rename it or "
                            "set the bundle variable `schema` to a new name.", "Re-run the job."],
                context=ctx) from err
        return False
