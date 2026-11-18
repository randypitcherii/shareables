"""Upsert the bundle's Lakewatch detection rules from rules/*.yml.

The YAML is the same shape the Lakewatch rule editor shows under "Edit as YAML".
When bundles gain a native Lakewatch rule resource, these files should move to
`resources:` with little change and this module can be deleted.
"""
from __future__ import annotations

import glob
import os
from concurrent.futures import ThreadPoolExecutor

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.common.types.fieldmask import FieldMask
from databricks.sdk.service.lakewatch import Rule

from .errors import SetupError

PREFIX = "databricks_security_infra_"
MUTABLE = ["description", "severity", "category", "fidelity", "objective", "mitre", "schedule", "batch", "output",
           "deduplication", "deduplication_time_window", "annotations", "timerange", "runbook", "references"]


def load_specs(rules_dir: str, subs: dict[str, str]) -> list[dict]:
    specs = []
    for path in sorted(glob.glob(os.path.join(rules_dir, "*.yml"))):
        text = open(path).read()
        for k, v in subs.items():
            text = text.replace("${" + k + "}", v)
        spec = yaml.safe_load(text)
        if not str(spec.get("rule_name", "")).startswith(PREFIX):
            raise ValueError(f"{path}: rule_name must start with '{PREFIX}' so this bundle's rules are easy to identify in Lakewatch")
        specs.append(spec)
    return specs


def prepare(spec: dict, catalog: str, schema: str, warehouse_id: str, enabled: bool) -> dict:
    spec = dict(spec)
    spec.update(catalog_name=catalog, schema_name=schema)
    spec.setdefault("batch", {})["warehouse_id"] = warehouse_id
    spec.setdefault("schedule", {})["enabled"] = enabled
    spec.setdefault("annotations", {}).update({"rpw.bundle": "lakewatch-getting-started", "rpw.rule": spec["rule_name"]})
    return spec


def upsert(w: WorkspaceClient, spec: dict) -> dict:
    rule = Rule.from_dict(spec)
    existing = next(iter(w.lakewatch.list_rules(spec["catalog_name"], schema_name=spec["schema_name"],
                                                filter=f'rule_name = "{spec["rule_name"]}"')), None)
    if existing:
        rule.name = existing.name
        out = w.lakewatch.update_rule(existing.name, rule, FieldMask([f for f in MUTABLE if f in spec]))
        return {"rule": spec["rule_name"], "action": "updated", "name": out.name}
    out = w.lakewatch.create_rule(rule)
    return {"rule": spec["rule_name"], "action": "created", "name": out.name}


def uses_findings_tables(spec: dict) -> bool:
    """Rules over the bundle's security infrastructure tables must wait for the first snapshot."""
    return "rpw_security_infra_" in str(spec.get("batch", {}).get("sql", {}).get("query", ""))


def register_all(w: WorkspaceClient, rules_dir: str, catalog: str, schema: str, warehouse_id: str,
                 enabled: bool, ctx: dict, select=None, max_workers: int = 4) -> list[dict]:
    subs = {"catalog": catalog, "schema": schema, "warehouse_id": warehouse_id}
    specs = [prepare(s, catalog, schema, warehouse_id, enabled) for s in load_specs(rules_dir, subs)
             if select is None or select(s)]
    if not specs:
        return []

    def one(spec):
        try:
            return upsert(w, spec)
        except Exception as e:  # noqa: BLE001 - collect, report all failures together
            return {"rule": spec["rule_name"], "action": "failed", "error": f"{type(e).__name__}: {str(e)[:300]}"}

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = list(pool.map(one, specs))
    failed = [r for r in results if r["action"] == "failed"]
    if failed:
        raise SetupError(
            "LWGS-E401",
            what_happened=f"{len(failed)} of {len(results)} detection rules failed to register: {failed}",
            likely_cause="A rule's SQL references a table the warehouse cannot read (system tables need "
                         "USE/SELECT on system.access), or the Lakewatch rule schema changed.",
            next_steps=["Grant the run-as identity SELECT on system.access.audit (and system.access.outbound_network).",
                        "Open the failing rule's YAML under rules/ and test its query in the SQL editor.",
                        "Re-run the job; rules that succeeded are updated in place, not duplicated."],
            context={**ctx, "results": results})
    return results
