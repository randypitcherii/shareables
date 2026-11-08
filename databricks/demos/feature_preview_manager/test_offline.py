# /// script
# requires-python = ">=3.10"
# dependencies = ["databricks-sdk", "pandas"]
# ///
"""Offline test: `uv run test_offline.py`.

Local harness: run the notebook source with fake dbutils/spark/clients to exercise
account scope, multi-workspace, transient 429 retries, hard failures, and skip logic."""
import pathlib, random, sys, types, time
import pandas as pd
import databricks.sdk as sdk

random.seed(7)
WIDGETS = dict(scope="account", account_id="acct-1", accounts_host="https://accounts.cloud.databricks.com",
               secret_scope="s", client_id_key="a", client_secret_key="b", workspace_filter="", max_workers="8",
               output_table="", enable_phases="BETA,PUBLIC_PREVIEW", enable_only_previews="", exclude_previews="p_excluded",
               include_account_scope="true", dry_run="false", confirm="ENABLE")


class Widgets:
    def __getattr__(self, n):  # text/dropdown/multiselect -> no-op
        return lambda *a, **k: None
    def get(self, k): return WIDGETS[k]


dbutils = types.SimpleNamespace(widgets=Widgets(), secrets=types.SimpleNamespace(get=lambda s, k: "x"))
shown = []
display = lambda df: shown.append(df)
spark = None


class FakeApi:
    """State: name -> {'explicit': bool|None, 'default': bool}. Injects 429s and hard failures."""
    def __init__(self, name, broken=False):
        self.name, self.broken, self.calls = name, broken, 0
        self.meta = [dict(name=f"p{i}", display_name=f"P{i}", preview_phase=ph)
                     for i, ph in enumerate(["BETA", "BETA", "PUBLIC_PREVIEW", "GA", "PRIVATE_PREVIEW"] * 3)]
        self.meta += [dict(name="p_excluded", preview_phase="BETA"), dict(name="not_preview")]
        self.state = {m["name"]: {"explicit": random.choice([None, None, True, False]), "default": random.random() < .4}
                      for m in self.meta}
        self.state["p_patch_fails"] = {"explicit": None, "default": False}
        self.meta.append(dict(name="p_patch_fails", preview_phase="BETA"))

    def do(self, method, path, query=None, body=None):
        self.calls += 1
        if self.broken and "settings-metadata" in path:
            raise Exception("PERMISSION_DENIED: not an admin")
        if random.random() < 0.15:
            raise Exception("429 TOO_MANY_REQUESTS: slow down")
        if path.endswith("settings-metadata"):
            tok = int((query or {}).get("page_token") or 0)
            page = self.meta[tok:tok + 5]
            return {"settings_metadata": page, **({"next_page_token": str(tok + 5)} if tok + 5 < len(self.meta) else {})}
        name = path.rsplit("/", 1)[1]
        st = self.state[name]
        if method == "PATCH":
            if name == "p_patch_fails":
                raise Exception("INVALID_PARAMETER_VALUE: cannot enable")
            st["explicit"] = body["boolean_val"]["value"]
            return {}
        out = {"name": name, "effective_boolean_val": {"value": st["explicit"] if st["explicit"] is not None else st["default"]}}
        if st["explicit"] is not None:
            out["boolean_val"] = {"value": st["explicit"]}
        return out


class FakeClient:
    def __init__(self, name, broken=False):
        self.api_client = FakeApi(name, broken)
        self.config = types.SimpleNamespace(host=f"https://{name}")


WS = [types.SimpleNamespace(workspace_id=i, workspace_name=f"ws{i}", workspace_status=types.SimpleNamespace(value="RUNNING"))
      for i in range(1, 5)]
WS.append(types.SimpleNamespace(workspace_id=99, workspace_name="dead", workspace_status=types.SimpleNamespace(value="CANCELLED")))


class FakeAccount(FakeClient):
    def __init__(self, **kw):
        super().__init__("account")
        self.workspaces = types.SimpleNamespace(list=lambda: WS)
    def get_workspace_client(self, ws):
        if ws.workspace_id == 3:
            return FakeClient("ws3", broken=True)   # not admin in this ws
        if ws.workspace_id == 4:
            raise Exception("auth init failed")
        return FakeClient(f"ws{ws.workspace_id}")


sdk.AccountClient = FakeAccount
time_sleep = time.sleep
time.sleep = lambda s: None  # make backoff instant

src = (pathlib.Path(__file__).parent / "feature_preview_manager.py").read_text()
g = dict(dbutils=dbutils, display=display, spark=spark)
exec(compile(src, "notebook", "exec"), g)

inv, att = g["inventory"], g["attempts"]
print("\nINVENTORY targets:", inv["target_name"].unique().tolist())
print("errors:\n", inv[inv.error.notna()][["target_name", "error"]].drop_duplicates().to_string())
assert "dead" not in set(inv.target_name)
assert inv[inv.target_name == "ws3"].error.str.contains("PERMISSION_DENIED").all()
assert inv[inv.target_name == "ws4"].error.str.contains("client init failed").all()
assert "not_preview" not in set(inv.preview)
ok = inv[inv.error.isna()]
assert ok.effective_enabled.notna().all(), "429s must have been retried away"
print("\nATTEMPTS:\n", att.status.value_counts().to_string())
assert set(att.preview) & {"p_excluded"} == set()
assert (att[att.preview == "p_patch_fails"].status == "FAILED").all()
assert set(att.phase) <= {"BETA", "PUBLIC_PREVIEW"}
# never touch manual values
planned = g["plan"]
assert not planned.manually_set.any()
assert (att.status == "ENABLED").sum() == len(att) - (att.status == "FAILED").sum()
print("ALL CHECKS PASSED")
