"""Entry point for the rpw_lakewatch_getting_started job (single serverless task).

Order of operations (each step is idempotent, so the job is safe to re-run):
  1. preflight      workspace admin, Unity Catalog
  2. observe        Beta preview toggle, existing Lakewatch environments, earlier preview setup   (parallel)
  3. decide         pure policy in rpw_lakewatch/state.py; STOP states explain and exit unchanged
  4. environment    create catalog on default storage, turn on the Beta, register + initialize     (only if new)
  5. folder         Lakewatch folder (schema) for this bundle's tables and rules
  6. content        security infra snapshot || audit-log rules, then findings rules                     (parallel)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(sys.argv[0] if sys.argv and sys.argv[0] else __file__)))

from databricks.sdk import WorkspaceClient  # noqa: E402

from rpw_lakewatch import VERSION  # noqa: E402
from rpw_lakewatch.errors import SetupError, unexpected  # noqa: E402
from rpw_lakewatch.platform import Platform  # noqa: E402
from rpw_lakewatch.state import Action, Observed, decide  # noqa: E402

PRODUCT = "rpw-lakewatch-getting-started"  # SDK user-agent; lets the bundle's API calls be counted


def beacon(result: str) -> None:
    """One cheap authenticated call whose user agent carries only the outcome code (e.g. rpw-result/LWGS-E201).

    Lets the bundle's maintainers count which setup errors happen in the field and fix the bundle.
    No error text, names or data are sent. Disable with --var error_beacon=false.
    """
    try:
        from databricks.sdk import useragent
        useragent.with_extra("rpw-result", result)
        WorkspaceClient(product=PRODUCT, product_version=VERSION).current_user.me()
    except Exception:  # noqa: BLE001 - never let telemetry affect the run
        pass


def log(step: str, **kv):
    print(json.dumps({"step": step, **kv}, default=str), flush=True)


def stop_error(code: str, plan_notes: list[str], a, ctx: dict, envs: list[str] | None = None) -> SetupError:
    if code == "LWGS-E101":
        return SetupError(
            code,
            what_happened="This workspace already has a Lakewatch configuration from an earlier preview, and no "
                          "Lakewatch Beta environment. The bundle made no changes.",
            likely_cause="The workspace was set up during the Lakewatch private preview. The Beta uses separate "
                         "resources (rules and datasources are not migrated automatically), so moving to it is a "
                         "deliberate decision for whoever owns the preview setup.",
            next_steps=["Talk to whoever set up the preview (and your Databricks account team) about moving to "
                        "the Beta. Preview resources keep working while the Beta is set up alongside.",
                        "When ready, set up the Beta from the app switcher > Lakewatch, then re-run this job. It "
                        "reuses that environment if you deploy with --var catalog=THAT_CATALOG.",
                        "Or, to have this job set up the Beta next to the preview, redeploy with "
                        "--var allow_with_prior_preview=true."],
            context=ctx)
    if code == "LWGS-E104":
        envs = sorted(envs or [])
        cmds = [f"databricks bundle deploy --var catalog={c} && databricks bundle run rpw_lakewatch_getting_started"
                for c in envs] or ["databricks api get /api/2.0/lakewatch/environments   # find the catalog first"]
        return SetupError(
            code,
            what_happened=f"Lakewatch is already set up, but on another catalog. {'; '.join(plan_notes)}. "
                          "The bundle made no changes.",
            likely_cause="The bundle's `catalog` variable (default rpw_lakewatch) must name the catalog of the "
                         "existing Lakewatch environment so the dashboard and rules land in it.",
            next_steps=[("Redeploy pointing at the existing environment, then re-run:" if len(cmds) == 1 else
                         "Pick the environment this bundle should use, then run the matching command:")] + cmds,
            context=ctx)
    raise ValueError(code)


def run(a) -> dict:
    w = WorkspaceClient(product=PRODUCT, product_version=VERSION)
    p = Platform(w)
    ctx = {**p.facts(), "bundle_version": VERSION, "catalog": a.catalog, "schema": a.schema,
           "warehouse_id": a.warehouse_id, "dry_run": a.dry_run}

    # 1. preflight
    step = "preflight"
    try:
        p.require_admin(ctx)
        p.require_unity_catalog(ctx)

        # 2. observe (independent reads)
        step = "observe"
        with ThreadPoolExecutor(max_workers=3) as pool:
            f_set, f_env, f_prior = pool.submit(p.beta_setting), pool.submit(p.environments), pool.submit(p.prior_preview_setup)
        obs = Observed(beta_setting=f_set.result(), environments=f_env.result(),
                       prior_preview_setup=f_prior.result(), target_catalog=a.catalog,
                       allow_with_prior_preview=a.allow_with_prior_preview)
        ctx["observed"] = obs.__dict__
        log(step, **obs.__dict__)

        # 3. decide
        step = "decide"
        plan = decide(obs)
        ctx["plan"] = {"action": plan.action.value, "enable_beta": plan.enable_beta, "notes": plan.notes}
        log(step, **ctx["plan"])
        if plan.action is Action.STOP:
            raise stop_error(plan.error_code, plan.notes, a, ctx, obs.environments)
        if a.dry_run:
            log("dry_run", message="No changes made.")
            return {"dry_run": True, **ctx["plan"]}

        # 4. environment (only when new)
        step = "environment"
        created = {}
        if plan.action is Action.CREATE_ENVIRONMENT:
            # Catalog first: if it can't be created (E201/E202), nothing has been turned on yet.
            created["catalog"] = p.ensure_catalog(a.catalog, ctx, a.catalog_storage_root or None)
            if plan.enable_beta:
                p.enable_beta(ctx)
                created["beta_preview"] = "turned on"
            envs = p.wait_for_environments_api(ctx)
            if a.catalog not in envs:
                if envs:  # someone set it up while we waited; do not create a second one
                    raise stop_error("LWGS-E104", [f"existing environments: {sorted(envs)}"], a, ctx, envs)
                p.register_environment(a.catalog, a.warehouse_id, ctx)
                created["environment"] = a.catalog
            p.ensure_initialized(a.catalog, a.warehouse_id, ctx)
        log(step, **created)

        # 5. folder
        step = "folder"
        created["folder"] = p.ensure_folder(a.catalog, a.schema, ctx)
        log(step, folder=f"{a.catalog}.{a.schema}", created=created["folder"])

        # 6. content: security infra snapshot in parallel with audit-log rules; findings-based rules last
        step = "content"
        from rpw_lakewatch import security_infra, rules
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        rules_dir = a.rules_dir if os.path.isdir(a.rules_dir) else a.rules_dir.replace("/Workspace", "", 1)
        kw = dict(w=w, rules_dir=rules_dir, catalog=a.catalog, schema=a.schema, warehouse_id=a.warehouse_id,
                  enabled=a.rules_enabled, ctx=ctx)
        with ThreadPoolExecutor(max_workers=2) as pool:
            f_post = pool.submit(security_infra.collect, spark, w, a.catalog, a.schema, a.long_running_days,
                                 a.max_token_days, ctx)
            f_audit = pool.submit(rules.register_all, **kw, select=lambda s: not rules.uses_findings_tables(s))
            snapshot, audit_rules = f_post.result(), f_audit.result()
        findings_rules = rules.register_all(**kw, select=rules.uses_findings_tables)
        result = {"created": created, "findings": snapshot, "rules": audit_rules + findings_rules,
                  "rules_enabled": a.rules_enabled}
        log("done", **result)
        return result
    except SetupError:
        raise
    except Exception as e:  # noqa: BLE001
        raise unexpected(step, e, ctx) from e


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--catalog-storage-root", default="")
    ap.add_argument("--warehouse-id", required=True)
    ap.add_argument("--rules-dir", required=True)
    ap.add_argument("--rules-enabled", default="false")
    ap.add_argument("--allow-with-prior-preview", default="false")
    ap.add_argument("--long-running-days", type=int, default=30)
    ap.add_argument("--max-token-days", type=int, default=90)
    ap.add_argument("--dry-run", default="false")
    ap.add_argument("--error-beacon", default="true")
    a = ap.parse_args(argv)
    for k in ("rules_enabled", "allow_with_prior_preview", "dry_run", "error_beacon"):
        setattr(a, k, str(getattr(a, k)).strip().lower() == "true")
    try:
        result = run(a)
    except SetupError as e:
        print("LAKEWATCH_GETTING_STARTED_RESULT " + json.dumps({"ok": False, "code": e.code}), flush=True)
        if a.error_beacon:
            beacon(e.code)
        # The full, AI-pasteable block is the run's error message, so `bundle run` prints it in the terminal and
        # the run page shows it. It starts with the code so it is searchable.
        raise RuntimeError(f"{e.code}: {e.title}\n\n{e.render()}") from None
    print("LAKEWATCH_GETTING_STARTED_RESULT " + json.dumps({"ok": True}), flush=True)
    if a.error_beacon:
        beacon("ok")
    if a.dry_run:
        print("\nDry run complete: the plan is printed above and nothing was changed. "
              "Run again without --params dry_run=true to apply it.", flush=True)
        return result
    print("\nLakewatch is ready. Open it from the app switcher (top-left grid icon) > Lakewatch. "
          + ("Rules are scheduled." if a.rules_enabled else
             "The detection rules are registered DISABLED: review and tune each one, then enable it in Lakewatch "
             "or redeploy with --var rules_enabled=true."), flush=True)
    return result


if __name__ == "__main__":
    main()
