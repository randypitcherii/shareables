"""Shared harness for setup, verify, and the numbered matrix rows.

Principals:
  owner        the human running make (DATABRICKS_CONFIG_PROFILE, OAuth U2M). Deploys; owns the services.
  test_caller  throwaway non-owner SP (scripts/setup/00_test_caller.py). Holds EXECUTE on sub_a only.

Every relayed call carries two credentials, the way Claude Code sends them through Unity Gateway:
  X-Databricks-AI-Gateway-Token: Bearer <databricks token>   who the gateway authorizes and bills usage to
  Authorization: Bearer <anthropic oauth token>              what the gateway relays to Anthropic

The matrix never holds a real Anthropic token. It sends a fake one, so Anthropic's own 401 is the
proof the request got past Unity Catalog and reached the relay.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import urllib.request
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parent.parent
RESULTS_FILE = ROOT / "results" / "matrix_results.json"
STATE_FILE = ROOT / ".state" / "test_caller.json"
PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
TARGET = os.environ.get("TARGET", "dev")
CATALOG = os.environ.get("CATALOG", "main")
SUBSCRIPTIONS = ("sub_a", "sub_b", "sub_c")
FAKE_ANTHROPIC_TOKEN = "sk-ant-oat01-not-a-real-token-multi-claude-sub-experiment"
PING = {"model": "claude-haiku-4-5", "max_tokens": 16,
        "messages": [{"role": "user", "content": "Reply with the single word: pong"}]}


# --- output ---------------------------------------------------------------------------------------
def section(title: str) -> None:
    print(f"\n== {title}")


def ok(msg: str) -> None:
    print(f"  ✅ {msg}")


def fail(msg: str) -> None:
    print(f"  ❌ {msg}")


# --- principals -----------------------------------------------------------------------------------
def load_test_caller() -> dict:
    if not STATE_FILE.exists():
        raise SystemExit("no test caller yet: run `make setup` first")
    return json.loads(STATE_FILE.read_text())


@lru_cache(maxsize=4)
def client(principal: str):
    from databricks.sdk import WorkspaceClient

    if principal == "owner":
        return WorkspaceClient(profile=PROFILE)
    if principal == "test_caller":
        tc = load_test_caller()
        # auth_type pinned so a configured profile can never stand in for the SP.
        return WorkspaceClient(host=client("owner").config.host, client_id=tc["application_id"],
                               client_secret=tc["client_secret"], auth_type="oauth-m2m")
    raise ValueError(principal)


def databricks_token(principal: str) -> str:
    header = client(principal).config.authenticate()["Authorization"]
    return header.removeprefix("Bearer ").strip()


def host() -> str:
    return client("owner").config.host.rstrip("/")


@lru_cache(maxsize=1)
def bundle_summary() -> dict:
    args = ["databricks", "bundle", "summary", "--profile", PROFILE, "--target", TARGET, "-o", "json",
            "--var", f"catalog={CATALOG}"]
    if STATE_FILE.exists():
        args += ["--var", f"sub_a_principal={load_test_caller()['application_id']}"]
    elif os.environ.get("SUB_A_PRINCIPAL"):
        args += ["--var", f"sub_a_principal={os.environ['SUB_A_PRINCIPAL']}"]
    out = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def schema() -> str:
    s = bundle_summary()["resources"]["schemas"]["msub"]
    return f"{s['catalog_name']}.{s['name']}"


def service(sub: str) -> str:
    """Fully qualified name of one subscription's provider service, e.g. main.multi_claude_sub.claude_sub_a."""
    return f"{schema()}.claude_{sub}"


# --- HTTP that never raises on status -------------------------------------------------------------
def http(method: str, path: str, headers: dict, body: dict | None = None) -> tuple[int, dict]:
    """(status, body). A 400/401/403/429 is data for the matrix, not an exception."""
    req = urllib.request.Request(f"{host()}{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json", **headers})
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            return r.status, json.loads(r.read() or b"{}")
    except HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, {"message": raw[:400]}
    except (URLError, TimeoutError) as e:
        return 0, {"message": f"unreachable: {getattr(e, 'reason', e)}"}


def api(principal: str, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    return http(method, path, {"Authorization": f"Bearer {databricks_token(principal)}"}, body)


def relayed_messages(principal: str, provider_service: str, anthropic_token: str | None = FAKE_ANTHROPIC_TOKEN,
                     swap_header: bool = True) -> tuple[int, dict]:
    """POST /ai-gateway/anthropic/v1/messages the way a relayed Claude Code request arrives.

    swap_header=False sends the Databricks token in Authorization instead (what a client unaware of
    the relay does); the gateway then has no Anthropic credential to forward.
    """
    dbx = databricks_token(principal)
    headers = {"Databricks-Model-Provider-Service": provider_service, "anthropic-version": "2023-06-01"}
    if swap_header:
        headers["X-Databricks-AI-Gateway-Token"] = f"Bearer {dbx}"
        if anthropic_token:
            headers["Authorization"] = f"Bearer {anthropic_token}"
            headers["anthropic-beta"] = "oauth-2025-04-20"
    else:
        headers["Authorization"] = f"Bearer {dbx}"
    return http("POST", "/ai-gateway/anthropic/v1/messages", headers, PING)


def upstream(body: dict) -> dict:
    """The MODEL_PROVIDER_ERROR metadata a relayed failure carries, or {}."""
    for d in body.get("details") or []:
        if isinstance(d, dict) and d.get("reason") == "MODEL_PROVIDER_ERROR":
            return d.get("metadata") or {}
    return {}


def classify(status: int, body: dict) -> str:
    """Where a relayed request stopped.

    reached_anthropic   past Unity Catalog; Anthropic itself answered (our fake token → its 401)
    denied_by_uc        Unity Catalog refused the caller (no EXECUTE / USE_SCHEMA)
    rate_limited        the gateway throttled it before the relay
    relay_refused       the service refused a request that carried no relayed credential
    """
    up = upstream(body)
    if up.get("provider") == "anthropic" and up.get("upstream_status"):
        return "reached_anthropic"
    if status == 403 or body.get("error_code") == "PERMISSION_DENIED":
        return "denied_by_uc"
    if status == 429:
        return "rate_limited"
    if status == 400 and "relayed" in str(body.get("message", "")).lower():
        return "relay_refused"
    if status == 200:
        return "ok"
    return f"other_{status}"


# --- SQL ------------------------------------------------------------------------------------------
def warehouse_id() -> str:
    if os.environ.get("WAREHOUSE_ID"):
        return os.environ["WAREHOUSE_ID"]
    whs = list(client("owner").warehouses.list())
    if not whs:
        raise SystemExit("no SQL warehouse: set WAREHOUSE_ID")
    return next((w.id for w in whs if w.enable_serverless_compute), whs[0].id)


def sql(statement: str) -> tuple[bool, list[list] | str]:
    s, d = api("owner", "POST", "/api/2.0/sql/statements",
               {"warehouse_id": warehouse_id(), "statement": statement, "wait_timeout": "50s"})
    if s != 200 or d.get("status", {}).get("state") != "SUCCEEDED":
        return False, str(d.get("status", {}).get("error", {}).get("message") or d.get("message") or s)[:300]
    return True, d.get("result", {}).get("data_array") or []


# --- results --------------------------------------------------------------------------------------
# results/ is committed to a public repo: identities, hosts, request ids and the schema's
# catalog are rewritten to role names before anything is written.
_REQ_ID = re.compile(r"(ReqId: )[A-Za-z0-9-]+|(req_)[A-Za-z0-9]+")


def redactions() -> dict[str, str]:
    roles = {host(): "<workspace>", host().removeprefix("https://"): "<workspace>"}
    try:
        roles[client("owner").current_user.me().user_name] = "<owner>"
    except Exception:  # noqa: BLE001 — redaction must never block recording
        pass
    if STATE_FILE.exists():
        roles[load_test_caller()["application_id"]] = "<test_caller>"
    try:
        roles[schema()] = "<schema>"
    except Exception:  # noqa: BLE001
        pass
    return roles


def redact(text: str, roles: dict[str, str]) -> str:
    for ident, role in sorted(roles.items(), key=lambda kv: -len(kv[0])):
        if ident:
            text = text.replace(ident, role)
    return _REQ_ID.sub(lambda m: f"{m.group(1) or m.group(2)}<id>", text)


def record(row: str, result: str, notes: str, evidence: dict | None = None,
           path: Path = RESULTS_FILE, roles: dict[str, str] | None = None) -> None:
    """Upsert one row into results/matrix_results.json. result is one of ✅ ❌ ◑ ❓."""
    assert result in {"✅", "❌", "◑", "❓"}, result
    roles = redactions() if roles is None else roles
    data = json.loads(path.read_text()) if path.exists() else {}
    data[row] = {"result": result, "notes": redact(notes, roles),
                 "evidence": json.loads(redact(json.dumps(evidence or {}, ensure_ascii=False), roles)),
                 "recorded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "target": TARGET}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
    print(f"  → {row}: {result} {redact(notes, roles)[:140]}")


def snip(body: dict, n: int = 220) -> str:
    return json.dumps(body)[:n]


if __name__ == "__main__":  # pragma: no cover
    sys.exit("import me")
