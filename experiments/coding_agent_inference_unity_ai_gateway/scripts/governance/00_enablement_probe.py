"""Preflight: which Unity AI Gateway governance features are ON in THIS workspace.

Most of the ❓ cells in this experiment's matrix are not "the test failed" —
they are "the feature that would make the test meaningful is not enabled on
this workspace". Between the July 2026 and August 2026 runs, Databricks
shipped several governance surfaces (foundation-model Unity Catalog
permissions, ABAC grant policies, model/MCP services and service policies,
the ``system.ai_gateway.usage`` table). Each has its own enablement gate, and
guessing which ones are live is how you end up demoing something that does
not work in a customer's workspace.

This script asks the workspace directly and prints one line per feature. It
is entirely read-only: no grants, no endpoint config, no mutations, no gate.
It records nothing into the matrix — it explains the matrix.

Run it first when re-validating on a new workspace.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _gov_common as gov  # noqa: E402
import _common  # noqa: E402


def _line(name: str, enabled: bool | None, detail: str) -> None:
    symbol = "❓" if enabled is None else ("✅ ON " if enabled else "❌ OFF")
    print(f"  {symbol}  {name}")
    print(f"          {detail}")


def probe_fm_uc_permissions() -> None:
    """Foundation-model UC permissions — 'GA but requires enablement'.

    This is the ``MODEL`` securable (registered models). It is a DIFFERENT
    feature from ``MODEL_SERVICE`` (the Unity AI Gateway securable probed
    below) — the two can be, and here are, in opposite states. With this
    feature off, Unity Catalog answers INVALID_STATE 'MODEL is not enabled'
    and system.ai models are only addressable as FUNCTION.
    """
    status, body = gov.api_request(
        "GET", "/api/2.1/unity-catalog/permissions/model/system.ai.claude-sonnet-4-6"
    )
    text = json.dumps(body) if not isinstance(body, str) else body
    if status == 200:
        _line("Foundation-model UC permissions (MODEL securable)", True,
              f"GET permissions/model -> 200 {gov.snip(text, 160)}")
    elif "not enabled" in text:
        _line("Foundation-model UC permissions (MODEL securable)", False,
              f"GET permissions/model -> {status} {gov.snip(text, 160)} — this is the "
              "gate behind matrix rows 3-6; enabling it is an account-team request.")
    else:
        _line("Foundation-model UC permissions (MODEL securable)", None,
              f"GET permissions/model -> {status} {gov.snip(text, 160)}")


def probe_system_ai_grants() -> None:
    """The default schema-wide EXECUTE grant that makes per-model grants moot."""
    status, body = gov.api_request(
        "GET", "/api/2.1/unity-catalog/permissions/schema/system.ai"
    )
    assignments = (body or {}).get("privilege_assignments", []) if isinstance(body, dict) else []
    broad = [
        a["principal"]
        for a in assignments
        if "EXECUTE" in (a.get("privileges") or [])
        and a.get("principal") in ("account users", "users")
    ]
    _line(
        "system.ai schema-wide EXECUTE grant",
        bool(broad),
        f"EXECUTE on system.ai held by {broad or 'no broad group'} — while a broad "
        "group holds it, a per-model GRANT is redundant and a REVOKE of one "
        "changes nothing (matrix row 3).",
    )


def probe_abac_policies() -> None:
    """ABAC GRANT policies — new in 2026, can grant EXECUTE FOR MODELS."""
    res = gov.sql_exec("SHOW POLICIES ON SCHEMA system.ai")
    if res["ok"]:
        _line("ABAC policy engine (SHOW/CREATE POLICY)", True,
              f"SHOW POLICIES ON SCHEMA system.ai parsed; {len(res['rows'])} policy(ies) "
              "defined. ABAC adds GRANT policies only — there is still no DENY form.")
    else:
        _line("ABAC policy engine (SHOW/CREATE POLICY)", False,
              f"SHOW POLICIES rejected: {gov.snip(res['error'], 160)}")


def probe_model_services() -> None:
    """Unity AI Gateway model services — the MODEL_SERVICE UC securable.

    Probe over REST, not SQL. There is no ``SHOW GRANTS ON MODEL SERVICE``
    DDL — asking SQL about it returns PARSE_SYNTAX_ERROR whether or not the
    feature is live, so a SQL probe reports a false OFF. The securable is
    real and carries its own per-principal ACL; that is what to ask.
    """
    status, body = gov.api_request(
        "GET",
        "/api/2.1/unity-catalog/model-services"
        "?catalog_name=system&schema_name=ai&max_results=200",
    )
    services = (body or {}).get("model_services", []) if isinstance(body, dict) else []
    if status != 200:
        _line("Model services (MODEL_SERVICE securable)", False,
              f"GET model-services -> {status} {gov.snip(body, 150)} — the Unity AI "
              "Gateway model catalog is not available on this workspace.")
        return

    # Show an ACL that actually carries a grant when one exists — a service
    # with no direct assignments reads as "ACL broken" when it only means
    # nobody granted on that specific service.
    acl_note = ""
    for service in services[:5]:
        name = service["name"].split("/")[-1]
        acl_status, acl = gov.api_request(
            "GET", f"/api/2.1/unity-catalog/permissions/model_service/{name}"
        )
        if acl_status != 200:
            acl_note = (
                f" Per-service ACL unreadable: permissions/model_service/{name} -> "
                f"HTTP {acl_status} {gov.snip(acl, 120)}."
            )
            break
        holders = [
            f"{a.get('principal')}:{','.join(a.get('privileges') or [])}"
            for a in ((acl or {}).get("privilege_assignments") or [])
        ]
        acl_note = (
            f" Per-service ACL is live: permissions/model_service/{name} -> "
            f"HTTP 200 {holders or '(no direct assignments on this one)'}."
        )
        if holders:
            break
    _line(
        "Model services (MODEL_SERVICE securable)",
        True,
        f"{len(services)} model service(s) in system.ai.{acl_note} This is the "
        "per-model ACL surface for the /ai-gateway/* routes — distinct from the "
        "MODEL securable above, and distinct from service policies below.",
    )


def probe_service_policies() -> None:
    """Service policies (Beta) — the only ALLOW/DENY/ASK surface for AI assets.

    Service policies are ABAC policies scoped to a MODEL_SERVICE or
    MCP_SERVICE. The honest test is whether the policy API accepts those
    securable types at all; its rejection message enumerates the types it
    does support, which is the most legible evidence available.
    """
    status, body = gov.api_request(
        "GET",
        "/api/2.1/unity-catalog/policies/MODEL_SERVICE/system.ai.claude-sonnet-4-6",
    )
    text = json.dumps(body) if not isinstance(body, str) else body
    if status == 200:
        count = len((body or {}).get("policies", []))
        _line("Service policies on MODEL_SERVICE (Beta)", True,
              f"policies API accepts MODEL_SERVICE; {count} policy(ies) attached. "
              "Built-in guardrails are system.ai.block_pii / block_unsafe_content / "
              "block_jailbreak / block_hallucination.")
        return
    # The rejection body is JSON; take only the human-readable type list.
    supported = ""
    if "Supported types" in text:
        supported = text.split("Supported types:", 1)[1].split('"', 1)[0].strip(" .")
    _line("Service policies on MODEL_SERVICE (Beta)", False,
          f"policies API rejects MODEL_SERVICE -> HTTP {status}"
          + (f"; it supports only: {supported}." if supported else f" {gov.snip(text, 150)}")
          + " Service policies are the guardrail path for /ai-gateway/* and the only "
          "ALLOW/DENY/ASK surface for model access, so rows 4-6 stay untestable here.")


def probe_gateway_usage_table() -> None:
    """system.ai_gateway.usage — the new per-agent attribution surface."""
    res = gov.sql_exec("DESCRIBE TABLE system.ai_gateway.usage")
    if not res["ok"]:
        _line("system.ai_gateway.usage (per-agent attribution)", False,
              f"table absent: {gov.snip(res['error'], 140)} — per-agent attribution "
              "falls back to the legacy usage_context body map.")
        return
    columns = [r[0] for r in res["rows"] if r and r[0] and not r[0].startswith("#")]
    _line("system.ai_gateway.usage (per-agent attribution)", True,
          f"present with {len(columns)} columns including "
          f"{[c for c in columns if c in ('requester', 'request_tags', 'user_agent', 'api_type')]} "
          "— request_tags is fed by the Databricks-Ai-Gateway-Request-Tags header (row 9).")


def probe_model_provider_services() -> None:
    """Model provider services — the documented Claude Code on-ramp."""
    host, token = _common.get_host(), _common.get_token()
    try:
        resp = requests.post(
            f"{host}/ai-gateway/anthropic/v1/messages",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Databricks-Model-Provider-Service": "system.ai.anthropic",
            },
            json={
                "model": "claude-sonnet-4-6",
                "messages": [{"role": "user", "content": "say pong"}],
                "max_tokens": 8,
            },
            timeout=60,
        )
    except requests.RequestException as exc:
        _line("Model provider services (Databricks-Model-Provider-Service)", None, str(exc)[:160])
        return
    ok = resp.status_code == 200
    _line(
        "Model provider services (Databricks-Model-Provider-Service)",
        ok,
        f"probe with system.ai.anthropic -> HTTP {resp.status_code} {resp.text[:130]}"
        + ("" if ok else " — the header-selected provider-service path is not "
           "provisioned here; point agents at a model id instead."),
    )


def main() -> int:
    _common.section("00 — Which governance features are enabled on this workspace?")
    print(f"  Host: {_common.get_host()}")
    try:
        gov.get_warehouse_id()
    except RuntimeError as exc:
        print(f"  (No SQL warehouse — SQL-based probes will be skipped: {exc})")

    probe_fm_uc_permissions()
    probe_system_ai_grants()
    probe_abac_policies()
    probe_model_services()
    probe_service_policies()
    probe_gateway_usage_table()
    probe_model_provider_services()

    print(
        "\n  Read this alongside the README matrix: an ❓ row usually means the "
        "feature above it is OFF here, not that the capability is absent from "
        "the product."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
