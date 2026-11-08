# Databricks notebook source
# MAGIC %md
# MAGIC # Feature preview inventory & bulk enablement
# MAGIC
# MAGIC Read the README next to this notebook first, especially **Caveats**.
# MAGIC
# MAGIC Runs on **serverless** compute. Uses the public **Settings v2 API**
# MAGIC ([docs](https://docs.databricks.com/aws/en/admin/workspace-settings/settings-api-manage)):
# MAGIC
# MAGIC | What | Endpoint |
# MAGIC |---|---|
# MAGIC | List previews (workspace) | `GET /api/2.1/settings-metadata` (entries with `preview_phase`) |
# MAGIC | Read / set (workspace) | `GET` / `PATCH /api/2.1/settings/{name}` |
# MAGIC | List / read / set (account) | `/api/2.1/accounts/{account_id}/settings-metadata`, `/api/2.1/accounts/{account_id}/settings/{name}` |
# MAGIC
# MAGIC **How state is read.** Each setting returns
# MAGIC - `effective_boolean_val` → what is **currently in effect** (what the Previews page shows)
# MAGIC - `boolean_val` → present **only if someone explicitly set it** (manual toggle / API). Absent ⇒ the preview is riding its **default**.
# MAGIC
# MAGIC **Sections**
# MAGIC 1. Configure (widgets) → 2. Inventory (read-only) → 3. *(optional)* Enable previews that are **OFF only because of their default**.
# MAGIC
# MAGIC **Auth**
# MAGIC - `scope = current_workspace` — no setup; uses the notebook's own identity (must be a workspace admin).
# MAGIC - `scope = account` — inventories the account **and every workspace** in it. Needs an OAuth M2M **service principal** that is an
# MAGIC   **account admin** *and* a **workspace admin in each workspace**; store its client ID / secret in a secret scope.

# COMMAND ----------

# MAGIC %md ## 1. Configure

# COMMAND ----------

dbutils.widgets.dropdown("scope", "current_workspace", ["current_workspace", "account"], "1. Scope")
dbutils.widgets.text("account_id", "", "2. Account ID (account scope)")
dbutils.widgets.text("accounts_host", "https://accounts.cloud.databricks.com", "3. Accounts host")
dbutils.widgets.text("secret_scope", "", "4. Secret scope (account scope)")
dbutils.widgets.text("client_id_key", "sp-client-id", "5. Secret key: SP client ID")
dbutils.widgets.text("client_secret_key", "sp-client-secret", "6. Secret key: SP client secret")
dbutils.widgets.text("workspace_filter", "", "7. Only these workspace IDs/names (comma sep, blank=all)")
dbutils.widgets.text("max_workers", "16", "8. Parallel API calls")
dbutils.widgets.text("output_table", "", "9. Save results to table (catalog.schema.table, blank=skip)")

# COMMAND ----------

import json, time, random, traceback
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from databricks.sdk import WorkspaceClient, AccountClient

W = lambda k: dbutils.widgets.get(k).strip()
SCOPE = W("scope")
MAX_WORKERS = max(1, int(W("max_workers") or 16))
WS_FILTER = {x.strip().lower() for x in W("workspace_filter").split(",") if x.strip()}
OUTPUT_TABLE = W("output_table")
RUN_TS = datetime.now(timezone.utc)

RETRYABLE = ("429", "TOO_MANY_REQUESTS", "TEMPORARILY_UNAVAILABLE", "500", "502", "503", "504",
             "timed out", "Timeout", "Connection", "RESOURCE_EXHAUSTED")


def call(client, method, path, *, query=None, body=None, attempts=6):
    """REST call with exponential backoff on throttling / transient errors. Raises on final failure."""
    for i in range(attempts):
        try:
            return client.api_client.do(method, path, query=query, body=body)
        except Exception as e:  # noqa: BLE001 — classify by message, SDK error classes vary by version
            msg = str(e)
            if i == attempts - 1 or not any(t in msg for t in RETRYABLE):
                raise
            time.sleep(min(30, (2 ** i) + random.random()))


def list_metadata(client, base):
    """Page through settings-metadata; return only feature previews (entries carrying preview_phase)."""
    out, token = [], None
    while True:
        q = {"page_size": 200, **({"page_token": token} if token else {})}
        r = call(client, "GET", f"{base}/settings-metadata", query=q) or {}
        out += [m for m in r.get("settings_metadata", []) if m.get("preview_phase")]
        token = r.get("next_page_token")
        if not token:
            return out


def short_err(e):
    s = str(e).replace("\n", " ")
    return s[:300]

# COMMAND ----------

# Build the list of targets: (scope_type, id, name, client, api_base)
targets = []
if SCOPE == "current_workspace":
    w = WorkspaceClient()
    wid = str(w.get_workspace_id())
    targets.append(("WORKSPACE", wid, w.config.host.replace("https://", ""), w, "/api/2.1"))
else:
    acct_id = W("account_id")
    assert acct_id and W("secret_scope"), "account scope needs account_id and secret_scope widgets"
    a = AccountClient(
        host=W("accounts_host"), account_id=acct_id,
        client_id=dbutils.secrets.get(W("secret_scope"), W("client_id_key")),
        client_secret=dbutils.secrets.get(W("secret_scope"), W("client_secret_key")),
    )
    targets.append(("ACCOUNT", acct_id, "(account)", a, f"/api/2.1/accounts/{acct_id}"))
    for ws in a.workspaces.list():
        if str(ws.workspace_status.value if ws.workspace_status else "") not in ("RUNNING", ""):
            continue
        if WS_FILTER and str(ws.workspace_id) not in WS_FILTER and (ws.workspace_name or "").lower() not in WS_FILTER:
            continue
        try:
            targets.append(("WORKSPACE", str(ws.workspace_id), ws.workspace_name, a.get_workspace_client(ws), "/api/2.1"))
        except Exception as e:  # keep going; record as an error row later
            targets.append(("WORKSPACE", str(ws.workspace_id), ws.workspace_name, e, "/api/2.1"))

print(f"{len(targets)} target(s): " + ", ".join(f"{t[0].lower()}:{t[2]}" for t in targets))

# COMMAND ----------

# MAGIC %md ## 2. Inventory (read-only)

# COMMAND ----------

def inventory_target(t):
    """Return rows for every preview in one account/workspace. Never raises — errors become rows."""
    kind, tid, tname, client, base = t
    base_row = dict(scope=kind, target_id=tid, target_name=tname)
    if isinstance(client, Exception):
        return [dict(base_row, preview=None, error=f"client init failed: {short_err(client)}")]
    try:
        metas = list_metadata(client, base)
    except Exception as e:
        return [dict(base_row, preview=None, error=f"list metadata failed: {short_err(e)}")]

    def one(m):
        row = dict(base_row, preview=m["name"], display_name=m.get("display_name"),
                   phase=m.get("preview_phase"), docs_link=m.get("docs_link"))
        try:
            s = call(client, "GET", f"{base}/settings/{m['name']}") or {}
            eff = (s.get("effective_boolean_val") or {}).get("value")
            explicit = "boolean_val" in s
            exp_val = (s.get("boolean_val") or {}).get("value", False) if explicit else None
            row.update(
                effective_enabled=bool(eff) if eff is not None else None,
                manually_set=explicit,
                manual_value=exp_val,
                on_default=not explicit,
                off_by_default=(not explicit) and eff is False,
                error=None,
            )
        except Exception as e:
            row.update(error=short_err(e))
        return row

    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        return list(ex.map(one, metas))


rows, t0 = [], time.time()
for i, t in enumerate(targets, 1):
    r = inventory_target(t)
    rows += r
    errs = sum(1 for x in r if x.get("error"))
    on = sum(1 for x in r if x.get("effective_enabled"))
    print(f"[{i}/{len(targets)}] {t[0].lower():9} {t[2]:<40} previews={len(r):>4} on={on:>4} "
          f"off_by_default={sum(1 for x in r if x.get('off_by_default')):>4} errors={errs}  ({time.time()-t0:.0f}s)")

cols = ["scope", "target_id", "target_name", "preview", "display_name", "phase", "effective_enabled",
        "manually_set", "manual_value", "on_default", "off_by_default", "docs_link", "error"]
inventory = pd.DataFrame(rows).reindex(columns=cols)
for c in ("effective_enabled", "manually_set", "on_default", "off_by_default"):
    inventory[c] = inventory[c].astype("boolean")  # nullable bool: None stays NA (error rows)
inventory["inventoried_at"] = RUN_TS
print(f"\n{len(inventory)} rows, {inventory['error'].notna().sum()} errors")

# COMMAND ----------

# MAGIC %md ### Full inventory
# MAGIC One row per (account/workspace, preview). Filter in the table UI, or use the summaries below.

# COMMAND ----------

display(inventory)

# COMMAND ----------

# MAGIC %md ### Summary per preview — where is it OFF only because of its default?

# COMMAND ----------

ok = inventory[inventory["error"].isna() & inventory["preview"].notna()]
summary = (ok.groupby(["scope", "preview", "display_name", "phase"], dropna=False)
             .agg(targets=("target_id", "nunique"),
                  on=("effective_enabled", lambda s: int(s.fillna(False).sum())),
                  manually_on=("manual_value", lambda s: int((s == True).sum())),   # noqa: E712
                  manually_off=("manual_value", lambda s: int((s == False).sum())),  # noqa: E712
                  off_by_default=("off_by_default", lambda s: int(s.fillna(False).sum())))
             .reset_index()
             .astype({"on": int, "manually_on": int, "manually_off": int, "off_by_default": int})
             .sort_values(["off_by_default", "preview"], ascending=[False, True]))
display(summary)

# COMMAND ----------

# MAGIC %md ### Errors (if any)

# COMMAND ----------

errs = inventory[inventory["error"].notna()][["scope", "target_id", "target_name", "preview", "error"]]
display(errs if len(errs) else pd.DataFrame({"result": ["no errors"]}))

# COMMAND ----------

if OUTPUT_TABLE:
    spark.createDataFrame(inventory.astype(str).replace({"None": None, "nan": None})) \
         .write.mode("append").option("mergeSchema", "true").saveAsTable(f"{OUTPUT_TABLE}_inventory")
    print(f"saved inventory -> {OUTPUT_TABLE}_inventory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. *(Optional)* Enable previews that are OFF only because of their default
# MAGIC
# MAGIC Targets = rows where `off_by_default = true` (never touched by anyone **and** currently off), narrowed by the widgets below.
# MAGIC **Previews someone explicitly turned OFF are never touched.**
# MAGIC
# MAGIC Safety: runs as a **dry run** unless `dry_run = false` **and** `confirm = ENABLE`. Each change is re-read after the PATCH to verify.
# MAGIC Every attempt is recorded (before/after) so you have an audit trail.
# MAGIC
# MAGIC > ⚠️ Enabling Beta / Private Preview features changes product behaviour. Review the dry-run list first.

# COMMAND ----------

dbutils.widgets.multiselect("enable_phases", "BETA", ["PRIVATE_PREVIEW", "BETA", "PUBLIC_PREVIEW", "GA_SOON", "GA"], "E1. Phases to enable")
dbutils.widgets.text("enable_only_previews", "", "E2. Only these previews (comma sep, blank=all)")
dbutils.widgets.text("exclude_previews", "", "E3. Never these previews (comma sep)")
dbutils.widgets.dropdown("include_account_scope", "false", ["false", "true"], "E4. Include account-level previews")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "E5. Dry run")
dbutils.widgets.text("confirm", "", "E6. Type ENABLE to apply")

# COMMAND ----------

PHASES = {p for p in W("enable_phases").split(",") if p}
ONLY = {x.strip() for x in W("enable_only_previews").split(",") if x.strip()}
EXCL = {x.strip() for x in W("exclude_previews").split(",") if x.strip()}
APPLY = W("dry_run") == "false" and W("confirm") == "ENABLE"

plan = ok[ok["off_by_default"].fillna(False).astype(bool) & ok["phase"].isin(PHASES)]
if W("include_account_scope") != "true":
    plan = plan[plan["scope"] == "WORKSPACE"]
if ONLY:
    plan = plan[plan["preview"].isin(ONLY)]
if EXCL:
    plan = plan[~plan["preview"].isin(EXCL)]

print(f"{'APPLYING' if APPLY else 'DRY RUN'}: {len(plan)} enablement(s) across "
      f"{plan['target_id'].nunique()} target(s), {plan['preview'].nunique()} preview(s), phases={sorted(PHASES)}")
display(plan[["scope", "target_name", "preview", "display_name", "phase", "effective_enabled"]])

# COMMAND ----------

clients = {(t[0], t[1]): (t[3], t[4]) for t in targets}


def enable_one(r):
    client, base = clients[(r["scope"], r["target_id"])]
    res = dict(scope=r["scope"], target_id=r["target_id"], target_name=r["target_name"], preview=r["preview"],
               phase=r["phase"], before_effective=r["effective_enabled"], attempted_at=datetime.now(timezone.utc),
               dry_run=not APPLY, status=None, after_effective=None, after_manual_value=None, error=None)
    if not APPLY:
        res["status"] = "DRY_RUN"
        return res
    try:
        # re-check right before writing: skip if someone set it in the meantime
        cur = call(client, "GET", f"{base}/settings/{r['preview']}") or {}
        if "boolean_val" in cur:
            res.update(status="SKIPPED_NOW_MANUALLY_SET",
                       after_effective=(cur.get("effective_boolean_val") or {}).get("value"),
                       after_manual_value=cur["boolean_val"].get("value"))
            return res
        call(client, "PATCH", f"{base}/settings/{r['preview']}",
             body={"name": r["preview"], "boolean_val": {"value": True}})
        chk = call(client, "GET", f"{base}/settings/{r['preview']}") or {}
        eff = (chk.get("effective_boolean_val") or {}).get("value")
        res.update(after_effective=eff, after_manual_value=(chk.get("boolean_val") or {}).get("value"),
                   status="ENABLED" if eff else "SET_BUT_NOT_EFFECTIVE")
    except Exception as e:
        res.update(status="FAILED", error=short_err(e))
    return res


results, done, t0 = [], 0, time.time()
with ThreadPoolExecutor(MAX_WORKERS if APPLY else 1) as ex:
    futs = [ex.submit(enable_one, r) for r in plan.to_dict("records")]
    for f in as_completed(futs):
        try:
            res = f.result()
        except Exception as e:  # belt & braces: enable_one already catches
            res = dict(status="FAILED", error=short_err(e))
        results.append(res)
        done += 1
        if APPLY and (res["status"] != "ENABLED" or done % 25 == 0 or done == len(futs)):
            print(f"[{done}/{len(futs)}] {res.get('status'):<26} {res.get('target_name')} / {res.get('preview')}"
                  f"{'  ' + res['error'] if res.get('error') else ''}  ({time.time()-t0:.0f}s)")

attempts = pd.DataFrame(results)
if len(attempts):
    print("\n" + attempts["status"].value_counts().to_string())

# COMMAND ----------

# MAGIC %md ### Enablement results

# COMMAND ----------

display(attempts if len(attempts) else pd.DataFrame({"result": ["nothing to enable"]}))

# COMMAND ----------

if len(attempts):
    display(attempts.pivot_table(index="target_name", columns="status", values="preview", aggfunc="count", fill_value=0).reset_index())

if OUTPUT_TABLE and len(attempts) and APPLY:
    spark.createDataFrame(attempts.astype(str).replace({"None": None, "nan": None})) \
         .write.mode("append").option("mergeSchema", "true").saveAsTable(f"{OUTPUT_TABLE}_enable_attempts")
    print(f"saved attempts -> {OUTPUT_TABLE}_enable_attempts")

# COMMAND ----------

# MAGIC %md
# MAGIC **Retry failures:** just re-run sections 2 → 3. Anything enabled is now `manually_set = true` and drops out of the plan,
# MAGIC so a re-run only retries what's left.
