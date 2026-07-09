"""Shared helpers for the Coding Agent Inference over Unity AI Gateway experiment.

Auth model: SSO/OAuth only — tokens come from `bin/get-databricks-token.sh`,
which mints short-lived OAuth access tokens. PATs (`dapi...`) are explicitly
rejected. Models are addressed by three-part Unity Catalog identifiers
(e.g. `system.ai.claude-sonnet-4-6`).

Dependencies: stdlib + requests.
"""

from __future__ import annotations

import configparser
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import requests

SEPARATOR = "=" * 60

# Experiment root is the parent of scripts/, regardless of cwd.
EXPERIMENT_ROOT = Path(__file__).resolve().parent.parent
TOKEN_HELPER = EXPERIMENT_ROOT / "bin" / "get-databricks-token.sh"
RESULTS_FILE = EXPERIMENT_ROOT / "results" / "matrix_results.json"

# Matches scripts/verify.py — the registered system.ai name drops the
# `databricks-` prefix of the legacy endpoint name (live-tested 2026-07-09).
DEFAULT_MODEL = "system.ai.claude-sonnet-4-6"

_THREE_PART_RE = re.compile(
    r"^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9._-]+$"
)


# ---------------------------------------------------------------------------
# Output helpers (style matches experiments/aws-databricks-gcs-access)
# ---------------------------------------------------------------------------

def section(title: str) -> None:
    """Print a section header."""
    print(f"\n{SEPARATOR}")
    print(title)
    print(SEPARATOR)


def ok(msg: str) -> None:
    """Print a success line."""
    print(f"  ✅ {msg}")


def fail(msg: str) -> None:
    """Print a failure line."""
    print(f"  ❌ {msg}")


# ---------------------------------------------------------------------------
# Profile / host / token resolution
# ---------------------------------------------------------------------------

def get_profile() -> str:
    """Return the Databricks CLI profile name (env first, default DEFAULT)."""
    return os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")


def get_host() -> str:
    """Resolve the workspace host URL.

    Precedence: DATABRICKS_HOST env var, then the profile's `host` entry in
    ~/.databrickscfg. Raises RuntimeError with guidance if neither resolves.
    """
    host = os.environ.get("DATABRICKS_HOST")
    if not host:
        profile = get_profile()
        cfg_path = Path(
            os.environ.get("DATABRICKS_CONFIG_FILE", Path.home() / ".databrickscfg")
        ).expanduser()
        if cfg_path.exists():
            # configparser treats [DEFAULT] as its magic defaults section, so
            # has_section("DEFAULT") is always False. Renaming the defaults
            # section makes [DEFAULT] a regular, addressable profile.
            cfg = configparser.ConfigParser(default_section="__unused__")
            cfg.read(cfg_path)
            if cfg.has_section(profile) and cfg.has_option(profile, "host"):
                host = cfg.get(profile, "host")
    if not host:
        raise RuntimeError(
            "Could not resolve the Databricks workspace host. Set the "
            "DATABRICKS_HOST env var, or add a `host = https://...` entry "
            f"under the [{get_profile()}] profile in ~/.databrickscfg "
            "(created by `databricks auth login`)."
        )
    return host.rstrip("/")


def get_token() -> str:
    """Mint a short-lived OAuth access token via bin/get-databricks-token.sh.

    Raises RuntimeError (with the helper's stderr message) if SSO auth is
    unavailable, and rejects any `dapi` PAT — this experiment is OAuth-only.
    """
    if not TOKEN_HELPER.exists():
        raise RuntimeError(
            f"Token helper not found at {TOKEN_HELPER}. "
            "Run scripts from a full checkout of the experiment."
        )
    proc = subprocess.run(
        ["bash", str(TOKEN_HELPER)],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        stderr = proc.stderr.strip() or "(no stderr output)"
        raise RuntimeError(
            f"get-databricks-token.sh failed (exit {proc.returncode}): {stderr}"
        )
    token = proc.stdout.strip()
    if not token:
        raise RuntimeError(
            "get-databricks-token.sh succeeded but printed an empty token."
        )
    if token.startswith("dapi"):
        raise RuntimeError(
            "Refusing to use a Databricks personal access token (dapi...). "
            "This experiment is SSO/OAuth-only — run `databricks auth login` "
            "and remove any DATABRICKS_TOKEN PAT from your environment."
        )
    return token


# ---------------------------------------------------------------------------
# Model identifier validation
# ---------------------------------------------------------------------------

def resolve_model_id(model_id: str | None = None) -> str:
    """Resolve and validate a three-part Unity Catalog model identifier.

    Defaults to the RPW_MODEL env var, then DEFAULT_MODEL (matching
    scripts/verify.py). Rejects legacy single-string endpoint
    names with a migration hint (e.g. `databricks-claude-sonnet-4-6` →
    `system.ai.claude-sonnet-4-6`).
    """
    if model_id is None:
        model_id = os.environ.get("RPW_MODEL", "").strip() or DEFAULT_MODEL
    if _THREE_PART_RE.match(model_id):
        return model_id
    if "." not in model_id:
        raise ValueError(
            f"'{model_id}' looks like a legacy single-string endpoint name. "
            "Unity AI Gateway addresses models by three-part Unity Catalog "
            "identifiers (<catalog>.<schema>.<model>). Migrate the old name "
            f"to its UC form, e.g. `databricks-claude-sonnet-4-6` → "
            f"`system.ai.claude-sonnet-4-6` (for '{model_id}', "
            f"try `system.ai.{model_id}`)."
        )
    raise ValueError(
        f"'{model_id}' is not a valid three-part Unity Catalog model "
        "identifier. Expected <catalog>.<schema>.<model>, e.g. "
        "`system.ai.claude-sonnet-4-6`."
    )


# ---------------------------------------------------------------------------
# Inference call
# ---------------------------------------------------------------------------

def chat_completion(
    model_id: str,
    prompt: str,
    max_tokens: int = 32,
    extra_headers: dict | None = None,
    extra_payload: dict | None = None,
) -> tuple[int, dict | str]:
    """Send one chat completion, trying each candidate route in order.

    Three-part UC identifiers (`system.ai.*`) are served by the Unity AI
    Gateway workspace routes, not the classic serving routes, so the route
    order for them is:

      1. `{host}/ai-gateway/mlflow/v1/chat/completions`  (Unity AI Gateway)
      2. `{host}/serving-endpoints/chat/completions`      (OpenAI-compatible)
      3. `{host}/serving-endpoints/{model_id}/invocations` (legacy per-endpoint)

    Legacy single-string names (only reachable when a caller deliberately
    bypasses resolve_model_id) skip the gateway route. Each attempt prints a
    one-line evidence trail; the next route is tried only on 404.

    Returns (status_code, parsed-json-or-text). Never raises on non-200 —
    governance tests need to observe 403s and other denials as data.
    """
    host = get_host()
    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    if extra_headers:
        headers.update(extra_headers)
    payload = {
        "model": model_id,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
    }
    if extra_payload:
        payload.update(extra_payload)

    # (url, include_model_in_payload)
    routes = [
        (f"{host}/ai-gateway/mlflow/v1/chat/completions", True),
        (f"{host}/serving-endpoints/chat/completions", True),
        (f"{host}/serving-endpoints/{model_id}/invocations", False),
    ]
    if not _THREE_PART_RE.match(model_id):
        routes = routes[1:]

    resp = None
    for url, include_model in routes:
        route_payload = dict(payload)
        if not include_model:
            # The invocations route encodes the model in the URL.
            route_payload.pop("model", None)
        resp = requests.post(url, headers=headers, json=route_payload, timeout=120)
        print(f"  → POST {url.removeprefix(host)} → HTTP {resp.status_code}")
        if resp.status_code != 404:
            break

    try:
        body: dict | str = resp.json()
    except ValueError:
        body = resp.text
    return resp.status_code, body


# ---------------------------------------------------------------------------
# Results matrix
# ---------------------------------------------------------------------------

def record_result(row_key: str, passed: bool | None, notes: str = "") -> None:
    """Upsert one row into results/matrix_results.json.

    passed=True → "✅", False → "❌", None → "❓". The file is pretty-printed
    with sorted keys so repeated runs produce clean diffs.
    """
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    results: dict = {}
    if RESULTS_FILE.exists():
        try:
            results = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            results = {}
    if not isinstance(results, dict):
        results = {}

    result_symbol = "❓" if passed is None else ("✅" if passed else "❌")
    results[row_key] = {
        "result": result_symbol,
        "notes": notes,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }

    RESULTS_FILE.write_text(
        json.dumps(results, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
