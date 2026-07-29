#!/usr/bin/env python3
"""gateway.py — mechanical companion for the bedrock-uc-ai-gateway skill.

Subcommands:
  preflight
      Checks: databricks CLI, auth state, uv availability, and whether the
      Bedrock key is present as a Databricks secret. The secret VALUE is never
      read or printed — only its presence in the scope's key list. Exits
      non-zero if a blocking check (CLI / auth / secret) fails.

  status
      GETs the model provider service, the model service, and both `_payload`
      inference tables, and prints a curated state table. Also flags orphaned
      `_payload` tables (table present, owning service gone) — the collision
      that breaks a recreate.

  invoke "<prompt>" [--model <id>] [--max-tokens N]
      Calls the model through the gateway via provider passthrough (Bedrock's
      native Converse API), retrying past the routing-cache lag, and prints
      the reply on stdout (usage goes to stderr).

Parameters (catalog, schema, provider/service ids, model, secret scope/key)
are read out of ../../deploy.py's parameter block, so this helper never drifts
from what was actually deployed. Edit deploy.py; this follows.

No third-party dependencies: python3 stdlib + the Databricks CLI. Auth is
whatever the CLI is already configured with — this script never creates,
stores, or prints a credential. Output is curated per-field rather than raw
API JSON, so a stored provider key can never be echoed to a terminal or log.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

DEPLOY_PY = Path(__file__).resolve().parents[2] / "deploy.py"

MPS_API = "/api/2.1/unity-catalog/model-provider-services"
MS_API = "/api/2.1/unity-catalog/model-services"
TABLES_API = "/api/2.1/unity-catalog/tables"

REQUIRED_PARAMS = (
    "TARGET_CATALOG",
    "TARGET_SCHEMA",
    "SECRET_SCOPE",
    "SECRET_KEY",
    "PROVIDER_ID",
    "SERVICE_ID",
    "MODEL_ID",
)


def die(msg: str, code: int = 2) -> "None":
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(code)


def row(check: str, status: str, detail: str = "") -> None:
    print(f"{check:<42} {status:<8} {detail}")


# ── deploy.py is the single source of truth for parameters ──────────────────


def deploy_params() -> dict:
    """Literal module-level assignments from deploy.py's parameter block.

    Computed values (f-strings, os.environ lookups) are skipped — they are
    derived here instead, the same way deploy.py derives them.
    """
    try:
        tree = ast.parse(DEPLOY_PY.read_text())
    except OSError as exc:
        die(f"cannot read {DEPLOY_PY}: {exc}")

    params: dict = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        try:
            params[target.id] = ast.literal_eval(node.value)
        except (ValueError, SyntaxError, TypeError):
            continue  # e.g. BEDROCK_REGION = os.environ.get(...)

    missing = [p for p in REQUIRED_PARAMS if p not in params]
    if missing:
        die(f"{DEPLOY_PY.name} is missing expected parameters: {', '.join(missing)}")

    params["SCHEMA_FQN"] = f"{params['TARGET_CATALOG']}.{params['TARGET_SCHEMA']}"
    params["PROVIDER_FQN"] = f"{params['SCHEMA_FQN']}.{params['PROVIDER_ID']}"
    params["SERVICE_FQN"] = f"{params['SCHEMA_FQN']}.{params['SERVICE_ID']}"
    params["BEDROCK_REGION"] = os.environ.get("BEDROCK_REGION", "us-east-1")
    return params


# ── Databricks CLI plumbing ─────────────────────────────────────────────────


def cli(args: list, profile: str | None, timeout: int = 60):
    """Run the Databricks CLI. Returns (returncode, stdout, stderr)."""
    if not shutil.which("databricks"):
        die("the Databricks CLI is not on PATH — install it, then re-run")
    cmd = ["databricks", *args]
    if profile:
        cmd += ["-p", profile]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def cli_json(args: list, profile: str | None):
    """Run a CLI command expected to emit JSON. Returns (obj, error_string)."""
    rc, out, err = cli(args, profile)
    if rc != 0:
        return None, (err or out or f"exit {rc}")
    if not out:
        return {}, None  # success, empty body (e.g. a scope with no secrets)
    try:
        return json.loads(out), None
    except json.JSONDecodeError:
        return None, f"unparseable CLI output: {out[:200]}"


def find_key(obj, key: str):
    """First value for `key` anywhere in a nested JSON structure."""
    if isinstance(obj, dict):
        if key in obj and isinstance(obj[key], str):
            return obj[key]
        for value in obj.values():
            found = find_key(value, key)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = find_key(item, key)
            if found:
                return found
    return None


def auth_describe(profile: str | None):
    obj, err = cli_json(["auth", "describe", "-o", "json"], profile)
    if err:
        return None, err
    return {
        "auth_type": find_key(obj, "auth_type"),
        "host": (find_key(obj, "host") or "").rstrip("/"),
        "username": find_key(obj, "username"),
    }, None


def workspace_host(profile: str | None) -> str:
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    if host:
        return host
    described, err = auth_describe(profile)
    if err or not described or not described["host"]:
        die(
            "cannot resolve the workspace host — run: "
            "databricks auth login --host https://<workspace-host>"
        )
    return described["host"]


def bearer_token(profile: str | None) -> str:
    """A short-lived OAuth token from the CLI. Never printed, never stored."""
    obj, err = cli_json(["auth", "token"], profile)
    if obj and obj.get("access_token"):
        return obj["access_token"]
    env_token = os.environ.get("DATABRICKS_TOKEN")
    if env_token:
        return env_token
    die(
        f"cannot mint a token ({err}) — run: "
        "databricks auth login --host https://<workspace-host>"
    )


def api_get(path: str, profile: str | None):
    """GET a workspace API path via the CLI. Returns (obj, error_string)."""
    return cli_json(["api", "get", path], profile)


def not_found(err: str) -> bool:
    # Underscores normalized so RESOURCE_DOES_NOT_EXIST and the prose form
    # ("... does not exist") both match.
    normalized = (err or "").lower().replace("_", " ")
    return any(
        marker in normalized for marker in ("not found", "does not exist", "404")
    )


# ── preflight ───────────────────────────────────────────────────────────────


def cmd_preflight(args, params) -> int:
    blocking = 0
    print(f"{'CHECK':<42} {'STATUS':<8} DETAIL")
    print(f"{'-----':<42} {'------':<8} ------")

    if shutil.which("databricks"):
        rc, out, _ = cli(["--version"], None)
        row("databricks CLI", "OK", out if rc == 0 else "found")
    else:
        row("databricks CLI", "FAIL", "not on PATH — install the Databricks CLI, then re-run")
        return blocking_exit(1)

    described, err = auth_describe(args.profile)
    if err or not described:
        row("auth", "FAIL", "not authenticated — run: databricks auth login --host https://<workspace-host>")
        blocking = 1
    else:
        detail = (
            f"auth_type={described['auth_type'] or '?'} "
            f"user={described['username'] or '?'} host={described['host'] or '?'}"
        )
        if described["auth_type"] == "pat":
            row("auth", "WARN", detail + " — house default is OAuth: databricks auth login")
        else:
            row("auth", "OK", detail)

    if shutil.which("uv"):
        row("uv", "OK", "deploy with: uv run deploy.py")
    else:
        row(
            "uv",
            "MISSING",
            "install uv, or: pip install 'databricks-sdk>=0.38' && python3 deploy.py",
        )

    scope, key = params["SECRET_SCOPE"], params["SECRET_KEY"]
    obj, err = cli_json(["secrets", "list-secrets", scope, "-o", "json"], args.profile)
    if err:
        row(
            f"secret {scope}/{key}",
            "FAIL",
            f"cannot list scope '{scope}' ({err.splitlines()[0][:120] if err else '?'}) — "
            f"the human runs: databricks secrets create-scope {scope}",
        )
        blocking = 1
    else:
        # The CLI flattens list responses to a JSON array; accept the wrapped
        # {"secrets": [...]} shape too.
        entries = obj.get("secrets", []) if isinstance(obj, dict) else (obj or [])
        keys = [e.get("key") for e in entries if isinstance(e, dict)]
        if key in keys:
            row(f"secret {scope}/{key}", "OK", "present (value never read by this script)")
        else:
            row(
                f"secret {scope}/{key}",
                "FAIL",
                f"missing — the human runs: databricks secrets put-secret {scope} {key} "
                "--string-value 'ABSK...'",
            )
            blocking = 1

    return blocking_exit(blocking)


def blocking_exit(blocking: int) -> int:
    if blocking:
        print("\npreflight: blocking check failed (see FAIL rows above).", file=sys.stderr)
    return 1 if blocking else 0


# ── status ──────────────────────────────────────────────────────────────────


def cmd_status(args, params) -> int:
    print(f"{'OBJECT':<42} {'STATUS':<8} DETAIL")
    print(f"{'------':<42} {'------':<8} ------")

    provider, perr = api_get(f"{MPS_API}/{params['PROVIDER_FQN']}", args.profile)
    if provider:
        config = provider.get("config", {}) or {}
        base_url = ((config.get("custom") or {}).get("direct") or {}).get("base_url", "?")
        targets = ", ".join(
            t.get("model", "?") for t in (config.get("targets") or [])
        ) or "none"
        row("provider service", "OK", params["PROVIDER_FQN"])
        row("  provider_type", "-", config.get("provider_type", "?"))
        row("  base_url", "-", base_url)
        row("  forward_unmanaged_paths", "-", str(config.get("forward_unmanaged_paths")))
        row("  allow_all_targets", "-", str(config.get("allow_all_targets")))
        row("  targets", "-", targets)
    elif not_found(perr):
        row("provider service", "MISSING", f"{params['PROVIDER_FQN']} — run: uv run deploy.py")
    else:
        row("provider service", "ERROR", (perr or "?").splitlines()[0][:120])

    service, serr = api_get(f"{MS_API}/{params['SERVICE_FQN']}", args.profile)
    if service:
        destinations = ((service.get("config", {}) or {}).get("routing") or {}).get(
            "destinations"
        ) or []
        row("model service", "OK", params["SERVICE_FQN"])
        for dest in destinations:
            external = dest.get("external_model_config") or {}
            model = (external.get("target") or {}).get("model", "?")
            row(
                f"  destination {dest.get('name', '?')}",
                "-",
                f"{dest.get('traffic_percentage', '?')}% → {model}",
            )
    elif not_found(serr):
        row("model service", "MISSING", f"{params['SERVICE_FQN']} — run: uv run deploy.py")
    else:
        row("model service", "ERROR", (serr or "?").splitlines()[0][:120])

    orphans = []
    for label, owner_present, table in (
        ("provider", bool(provider), f"{params['PROVIDER_FQN']}_payload"),
        ("model service", bool(service), f"{params['SERVICE_FQN']}_payload"),
    ):
        obj, err = api_get(f"{TABLES_API}/{table}", args.profile)
        short = table.split(".")[-1]
        if obj:
            if owner_present:
                row(f"inference table {short}", "OK", table)
            else:
                row(f"inference table {short}", "ORPHAN", f"{table} — owning {label} is gone")
                orphans.append(table)
        elif not_found(err):
            row(f"inference table {short}", "MISSING", f"{table} (created with the {label})")
        else:
            row(f"inference table {short}", "ERROR", (err or "?").splitlines()[0][:120])

    if orphans:
        print(
            "\nOrphaned inference table(s): "
            + ", ".join(orphans)
            + "\nRecreating the owning service will collide. Drop the table (or change "
            "table_name_prefix) before re-running deploy.py.",
            file=sys.stderr,
        )

    print(
        "\nPassthrough traffic logs to "
        f"{params['PROVIDER_FQN']}_payload. {params['SERVICE_FQN']}_payload stays empty "
        "until managed serving works for Claude on a CUSTOM provider (see SKILL.md)."
    )
    return 0


# ── invoke ──────────────────────────────────────────────────────────────────


def cmd_invoke(args, params) -> int:
    prompt = args.prompt
    if prompt == "-":
        prompt = sys.stdin.read()
    if not prompt.strip():
        die("empty prompt")

    model = args.model or params["MODEL_ID"]
    host = workspace_host(args.profile)
    url = f"{host}/ai-gateway/model/{model}/converse"
    payload = json.dumps(
        {
            "messages": [{"role": "user", "content": [{"text": prompt}]}],
            "inferenceConfig": {"maxTokens": args.max_tokens},
        }
    ).encode()

    request = urllib.request.Request(
        url,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {bearer_token(args.profile)}",
            "Databricks-Model-Provider-Service": params["PROVIDER_FQN"],
            "Content-Type": "application/json",
        },
    )

    deadline = time.time() + args.retry_seconds
    while True:
        try:
            with urllib.request.urlopen(request, timeout=args.timeout) as response:
                body = json.loads(response.read())
            break
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            # New/changed services take ~1-3 min to reach the gateway's
            # routing cache; 'Nodes do not exist' is that window.
            if "Nodes do not exist" in detail and time.time() < deadline:
                print("gateway routing cache not ready yet — retrying in 30s...", file=sys.stderr)
                time.sleep(30)
                continue
            die(explain_http_error(exc.code, detail, model, params), code=1)
        except urllib.error.URLError as exc:
            die(f"cannot reach {host}: {exc.reason}", code=1)

    try:
        text = body["output"]["message"]["content"][0]["text"]
    except (KeyError, IndexError, TypeError):
        die(f"unexpected response shape: {json.dumps(body)[:400]}", code=1)

    print(text)
    print(f"usage: {body.get('usage')}", file=sys.stderr)
    return 0


def explain_http_error(code: int, detail: str, model: str, params: dict) -> str:
    head = f"HTTP {code} from the gateway: {detail[:400]}"
    if code in (401, 403):
        return (
            head
            + "\nhint: Databricks-side auth. Re-login (databricks auth login --host ...), "
            "check the profile, then check permissions on the provider service."
        )
    if "UnknownOperationException" in detail or "model_not_found" in detail:
        return (
            head
            + "\nhint: that reads like a managed-serving route. Managed serving does not work "
            "for Claude on a CUSTOM+Bedrock provider — use provider passthrough "
            f"(/ai-gateway/model/{model}/converse + the "
            "Databricks-Model-Provider-Service header), which is what this script sends."
        )
    lowered = detail.lower()
    if "credential" in lowered or "security token" in lowered or "accessdenied" in lowered:
        return (
            head
            + "\nhint: AWS rejected the stored key. Create does not validate credentials, so a "
            "typo'd ABSK key only fails here. Fix the secret, then delete + recreate the "
            "provider (config.custom is not patchable) and drop its orphaned _payload table."
        )
    if "Nodes do not exist" in detail:
        return (
            head
            + "\nhint: still not in the gateway's routing cache after the retry window. That is "
            "usually ~1–3 min — wait and retry rather than recreating anything; if it persists, "
            "check the provider with: gateway.py status."
        )
    if code == 404:
        return (
            head
            + f"\nhint: check the provider exists — gateway.py status "
            f"(expected {params['PROVIDER_FQN']})."
        )
    return head


# ── main ────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="gateway.py",
        description="Companion helper for the bedrock-uc-ai-gateway skill.",
    )
    parser.add_argument(
        "--profile",
        default=os.environ.get("DATABRICKS_CONFIG_PROFILE"),
        help="Databricks CLI profile (default: DATABRICKS_CONFIG_PROFILE, else the CLI default)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("preflight", help="CLI, auth, uv, and Bedrock-secret checks")
    subparsers.add_parser("status", help="report provider, model service, and inference tables")

    invoke = subparsers.add_parser("invoke", help="call the model through provider passthrough")
    invoke.add_argument("prompt", help="prompt text, or '-' to read stdin")
    invoke.add_argument("--model", help="model id (default: MODEL_ID from deploy.py)")
    invoke.add_argument("--max-tokens", type=int, default=200)
    invoke.add_argument("--timeout", type=int, default=120, help="per-request timeout, seconds")
    invoke.add_argument(
        "--retry-seconds",
        type=int,
        default=300,
        help="how long to retry the routing-cache lag (default: 300)",
    )

    args = parser.parse_args()
    params = deploy_params()
    return {
        "preflight": cmd_preflight,
        "status": cmd_status,
        "invoke": cmd_invoke,
    }[args.command](args, params)


if __name__ == "__main__":
    sys.exit(main())
