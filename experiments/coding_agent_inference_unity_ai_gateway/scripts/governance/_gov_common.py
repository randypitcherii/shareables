"""Shared plumbing for the governance capability-matrix scripts.

These helpers sit ON TOP of ``scripts/_common.py`` (the experiment's source
of truth for SSO auth, model-id validation, inference, and results
recording) — they never replace it. Everything here uses the same
SSO/OAuth-only token path; zero PATs by construction.

What this module adds:

* a sys.path shim so scripts one level below ``scripts/`` can import
  ``_common`` (import ``_gov_common`` first, then ``import _common`` works);
* raw REST calls (``api_request``) against the workspace with the OAuth token;
* SQL Statements API execution (``sql_exec``) for GRANT/REVOKE/DENY probes
  and system-table queries;
* the ``ALLOW_ENDPOINT_MUTATION=1`` safety gate shared by scripts 03-08/10 —
  every mutation path defaults to dry-run unless explicitly opted in;
* the second-principal pattern (``TEST_PRINCIPAL`` / ``TEST_PRINCIPAL_PROFILE``)
  including ``as_profile()`` to run a query authenticated as that principal;
* ``conclude()`` — the one-and-only ``record_result`` call site per script.
"""

from __future__ import annotations

import contextlib
import json
import os
import sys
import time
from pathlib import Path

import requests

# --- sys.path shim: make scripts/ importable from scripts/governance/ ------
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import _common  # noqa: E402


# ---------------------------------------------------------------------------
# Safety gate for anything that mutates workspace state
# ---------------------------------------------------------------------------

MUTATION_ENV = "ALLOW_ENDPOINT_MUTATION"


def mutation_allowed() -> bool:
    """True only when the operator explicitly opted in to mutations."""
    return os.environ.get(MUTATION_ENV, "").strip() == "1"


def print_mutation_gate() -> None:
    """Explain the dry-run default for mutation-bearing scripts."""
    print(f"  {MUTATION_ENV} is not set to 1 — running in DRY-RUN mode.")
    print("  No grants, denies, permissions, or endpoint configs will be changed.")
    print(f"  To run the mutation path for real:  {MUTATION_ENV}=1 uv run python <this script>")


# ---------------------------------------------------------------------------
# Second-principal pattern (scripts 03-06)
# ---------------------------------------------------------------------------

def get_test_principal() -> str | None:
    """User email or group name that grants/denies target (env TEST_PRINCIPAL)."""
    value = os.environ.get("TEST_PRINCIPAL", "").strip()
    return value or None


def get_test_principal_profile() -> str | None:
    """databrickscfg profile authenticated AS the test principal (optional)."""
    value = os.environ.get("TEST_PRINCIPAL_PROFILE", "").strip()
    return value or None


def print_test_principal_setup() -> None:
    """Setup guidance when TEST_PRINCIPAL is missing."""
    print("  TEST_PRINCIPAL is not set — this row needs a second (target) principal.")
    print("  Setup:")
    print("    1. Pick (or create) a second workspace user or group with no special grants.")
    print("    2. export TEST_PRINCIPAL='second.user@example.com'   # or a group name")
    print("    3. Optional but recommended, to run the query-as-that-user step:")
    print("       databricks auth login --host <workspace-url> --profile test-principal")
    print("       (log in AS the second user), then export TEST_PRINCIPAL_PROFILE=test-principal")
    print(f"    4. Re-run with {MUTATION_ENV}=1 to allow the grant/revoke mutations.")


@contextlib.contextmanager
def as_profile(profile: str):
    """Temporarily switch _common's SSO token minting to another CLI profile.

    ``_common.get_token()`` shells out to ``bin/get-databricks-token.sh`` on
    every call and honors DATABRICKS_CONFIG_PROFILE, so swapping the env var
    is enough to make subsequent calls authenticate as the other principal.
    """
    saved = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    os.environ["DATABRICKS_CONFIG_PROFILE"] = profile
    try:
        yield
    finally:
        if saved is None:
            os.environ.pop("DATABRICKS_CONFIG_PROFILE", None)
        else:
            os.environ["DATABRICKS_CONFIG_PROFILE"] = saved


# ---------------------------------------------------------------------------
# Raw REST + SQL Statements API (same OAuth token as _common)
# ---------------------------------------------------------------------------

def api_request(
    method: str,
    path: str,
    payload: dict | None = None,
    token: str | None = None,
) -> tuple[int, dict | list | str]:
    """One workspace REST call. Returns (status, parsed-json-or-text).

    Never raises on non-2xx — governance rows need to observe 403s/404s as
    data. ``path`` is e.g. ``/api/2.0/serving-endpoints``.
    """
    host = _common.get_host()
    if token is None:
        token = _common.get_token()
    resp = requests.request(
        method.upper(),
        f"{host}{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=120,
    )
    try:
        body: dict | list | str = resp.json()
    except ValueError:
        body = resp.text
    return resp.status_code, body


_WAREHOUSE_ID: str | None = None


def get_warehouse_id() -> str:
    """Resolve a SQL warehouse for the Statements API.

    Precedence: DATABRICKS_WAREHOUSE_ID env var, else the first RUNNING
    warehouse, else the first warehouse of any state. Raises RuntimeError
    with guidance when none exists.
    """
    global _WAREHOUSE_ID
    if _WAREHOUSE_ID:
        return _WAREHOUSE_ID
    env_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "").strip()
    if env_id:
        _WAREHOUSE_ID = env_id
        return env_id
    status, body = api_request("GET", "/api/2.0/sql/warehouses")
    warehouses = body.get("warehouses", []) if isinstance(body, dict) else []
    if status != 200 or not warehouses:
        raise RuntimeError(
            "No SQL warehouse available for the SQL Statements API "
            f"(GET /api/2.0/sql/warehouses -> {status}). Set "
            "DATABRICKS_WAREHOUSE_ID to a warehouse id and re-run."
        )
    # Prefer an already-RUNNING warehouse; else serverless (starts in
    # seconds); else anything (classic warehouses can take minutes to start).
    running = [w for w in warehouses if w.get("state") == "RUNNING"]
    serverless = [w for w in warehouses if w.get("enable_serverless_compute")]
    _WAREHOUSE_ID = (running or serverless or warehouses)[0]["id"]
    return _WAREHOUSE_ID


def sql_exec(statement: str, token: str | None = None) -> dict:
    """Run one SQL statement via the Statements API using the OAuth token.

    Returns {"ok": bool, "state": str, "error": str, "columns": [...],
    "rows": [...]}. SQL-level failures set ok=False with the error message —
    they never raise, because refused GRANT/DENY syntax is exactly the data
    these probes exist to capture.
    """
    warehouse_id = get_warehouse_id()
    status, body = api_request(
        "POST",
        "/api/2.0/sql/statements",
        payload={
            "statement": statement,
            "warehouse_id": warehouse_id,
            "wait_timeout": "30s",
            "on_wait_timeout": "CONTINUE",
        },
        token=token,
    )
    if status != 200 or not isinstance(body, dict):
        return {
            "ok": False,
            "state": f"HTTP_{status}",
            "error": snip(body),
            "columns": [],
            "rows": [],
        }

    # Poll while the statement is still PENDING/RUNNING (post wait_timeout).
    # Generous deadline: a cold (even serverless) warehouse start is included.
    statement_id = body.get("statement_id")
    deadline = time.time() + 300
    while (
        isinstance(body, dict)
        and body.get("status", {}).get("state") in ("PENDING", "RUNNING")
        and time.time() < deadline
    ):
        time.sleep(2)
        status, body = api_request(
            "GET", f"/api/2.0/sql/statements/{statement_id}", token=token
        )
        if status != 200 or not isinstance(body, dict):
            return {
                "ok": False,
                "state": f"HTTP_{status}",
                "error": snip(body),
                "columns": [],
                "rows": [],
            }

    state = body.get("status", {}).get("state", "UNKNOWN")
    error = body.get("status", {}).get("error", {}).get("message", "")
    if state in ("PENDING", "RUNNING"):
        error = (
            "statement still {0} after 300s poll deadline (warehouse likely "
            "still starting) — re-run once a SQL warehouse is RUNNING".format(state)
        )
    columns = [
        c.get("name")
        for c in body.get("manifest", {}).get("schema", {}).get("columns", [])
    ]
    rows = body.get("result", {}).get("data_array", []) or []
    return {
        "ok": state == "SUCCEEDED",
        "state": state,
        "error": error,
        "columns": columns,
        "rows": rows,
    }


def sql_quote_principal(principal: str) -> str:
    """Backtick-quote a principal for GRANT/REVOKE/DENY SQL."""
    return "`" + principal.replace("`", "``") + "`"


def backtick_parts(full_name: str) -> str:
    """Backtick-quote each part of a dotted UC name (hyphens need quoting)."""
    return ".".join(f"`{p}`" for p in full_name.split("."))


_SECURABLE_CACHE: dict[str, str | None] = {}


def uc_model_securable(model_id: str) -> str | None:
    """Discover the UC registered-model securable behind a gateway model id.

    Live-tested 2026-07-10: the UC securables in system.ai KEEP the
    `databricks-` prefix (`system.ai.databricks-claude-sonnet-4-6` is the
    registered model) while the gateway INFERENCE identifier drops it
    (`system.ai.claude-sonnet-4-6`). The two names differ, so grants must
    target the discovered securable, not the inference id. Probes with
    SHOW GRANTS (read-only); returns the full securable name, or None.
    """
    if model_id in _SECURABLE_CACHE:
        return _SECURABLE_CACHE[model_id]
    catalog, schema, tail = model_id.split(".", 2)
    candidates = [model_id]
    if not tail.startswith("databricks-"):
        candidates.append(f"{catalog}.{schema}.databricks-{tail}")
    found: str | None = None
    for cand in candidates:
        probe = sql_exec(f"SHOW GRANTS ON FUNCTION {backtick_parts(cand)}")
        if probe["ok"]:
            found = cand
            break
    _SECURABLE_CACHE[model_id] = found
    return found


def try_privilege_statements(
    verb: str, model_id: str, principal: str, token: str | None = None
) -> tuple[bool, list[tuple[str, dict]]]:
    """Attempt GRANT/REVOKE/DENY EXECUTE against the model securable.

    Live-tested 2026-07-10: UC registered models take grants as securable
    type FUNCTION (`GRANT EXECUTE ON FUNCTION system.ai.`databricks-...``);
    a MODEL keyword is a parse error, and unquoted hyphenated names are too.
    The securable name is discovered via uc_model_securable() because it can
    differ from the gateway inference id. Returns
    (any_succeeded, [(statement, result), ...]) with verbatim errors kept.
    """
    verb = verb.upper()
    keyword = "FROM" if verb == "REVOKE" else "TO"
    quoted = sql_quote_principal(principal)
    attempts: list[tuple[str, dict]] = []
    securable = uc_model_securable(model_id)
    if securable is None:
        attempts.append((
            f"(securable discovery for {model_id})",
            {"ok": False, "state": "NOT_FOUND",
             "error": f"no UC registered model found for '{model_id}' "
                      f"(tried the id and its databricks- prefixed form)",
             "rows": []},
        ))
        return False, attempts
    stmt = f"{verb} EXECUTE ON FUNCTION {backtick_parts(securable)} {keyword} {quoted}"
    result = sql_exec(stmt, token=token)
    attempts.append((stmt, result))
    return bool(result["ok"]), attempts


# ---------------------------------------------------------------------------
# Serving-endpoint helpers
# ---------------------------------------------------------------------------

def endpoint_tail(model_id: str) -> str:
    """Map a three-part id to its legacy single-string endpoint name.

    The system.ai registered names drop the `databricks-` prefix that the
    legacy pay-per-token endpoint names carry (live-tested 2026-07-09:
    `system.ai.claude-sonnet-4-6` on the gateway route ↔ legacy endpoint
    `databricks-claude-sonnet-4-6`), so the mapping is tail + prefix.
    Override with LEGACY_ENDPOINT_NAME if a workspace deviates.
    """
    override = os.environ.get("LEGACY_ENDPOINT_NAME", "").strip()
    if override:
        return override
    tail = model_id.split(".")[-1]
    if not tail.startswith("databricks-"):
        tail = f"databricks-{tail}"
    return tail


def get_serving_endpoint(name: str, token: str | None = None) -> tuple[int, dict | list | str]:
    """GET one serving endpoint's full config (includes ai_gateway if set)."""
    return api_request("GET", f"/api/2.0/serving-endpoints/{name}", token=token)


def get_endpoint_id(name: str) -> str | None:
    """Resolve a serving endpoint's id (needed by the permissions API)."""
    status, body = get_serving_endpoint(name)
    if status == 200 and isinstance(body, dict):
        return body.get("id")
    return None


def principal_acl_field(principal: str) -> str:
    """Pick the permissions-API field for a principal string.

    Emails -> user_name; UUID-shaped -> service_principal_name; else group_name.
    """
    if "@" in principal:
        return "user_name"
    if len(principal) == 36 and principal.count("-") == 4:
        return "service_principal_name"
    return "group_name"


def get_endpoint_acl(endpoint_id: str) -> tuple[int, dict | list | str]:
    """GET the serving endpoint's permission ACL."""
    return api_request("GET", f"/api/2.0/permissions/serving-endpoints/{endpoint_id}")


def direct_acl_entries(acl_body: dict) -> list[dict]:
    """Extract the NON-inherited ACL entries in PUT-able form.

    Used to snapshot the original ACL so mutations can be reverted with a
    full-replace PUT (inherited permissions must not be re-PUT).
    """
    entries: list[dict] = []
    for item in acl_body.get("access_control_list", []):
        principal_fields = {
            k: v
            for k, v in item.items()
            if k in ("user_name", "group_name", "service_principal_name")
        }
        for perm in item.get("all_permissions", []):
            if not perm.get("inherited"):
                entries.append(
                    {**principal_fields, "permission_level": perm.get("permission_level")}
                )
    return entries


def put_endpoint_acl(endpoint_id: str, entries: list[dict]) -> tuple[int, dict | list | str]:
    """Full-replace the endpoint's direct ACL (used to restore/revoke)."""
    return api_request(
        "PUT",
        f"/api/2.0/permissions/serving-endpoints/{endpoint_id}",
        payload={"access_control_list": entries},
    )


def grant_endpoint_can_query(
    endpoint_id: str, principal: str
) -> tuple[int, dict | list | str]:
    """PATCH-add CAN_QUERY on the serving endpoint for the principal."""
    entry = {principal_acl_field(principal): principal, "permission_level": "CAN_QUERY"}
    return api_request(
        "PATCH",
        f"/api/2.0/permissions/serving-endpoints/{endpoint_id}",
        payload={"access_control_list": [entry]},
    )


# ---------------------------------------------------------------------------
# Output / verdict helpers
# ---------------------------------------------------------------------------

def collect_keys(obj, prefix: str = "") -> list[str]:
    """Flatten all nested key paths of a JSON object (for config-surface probes)."""
    keys: list[str] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else k
            keys.append(path)
            keys.extend(collect_keys(v, path))
    elif isinstance(obj, list):
        for item in obj:
            keys.extend(collect_keys(item, prefix + "[]"))
    return keys


def snip(obj, limit: int = 500) -> str:
    """Render any body/object to a single-line string capped at `limit`."""
    if isinstance(obj, str):
        text = obj
    else:
        try:
            text = json.dumps(obj, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError):
            text = repr(obj)
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


def conclude(row_key: str, passed: bool | None, notes: str) -> None:
    """Print the verdict line and record the matrix row exactly once."""
    symbol = "❓" if passed is None else ("✅" if passed else "❌")
    print(f"\n  VERDICT [{row_key}]: {symbol}")
    print(f"  NOTES: {notes}")
    _common.record_result(row_key, passed, notes)
    print(f"  Recorded to {_common.RESULTS_FILE}")
