"""Prove SSO-only auth to Databricks model serving with one real HTTP request.

Flow:
  1. Mint a short-lived OAuth token by shelling to bin/get-databricks-token.sh
     (or the .ps1 variant on Windows). Zero PATs — the helper refuses dapi tokens.
  2. Resolve the workspace host from DATABRICKS_HOST or the CLI profile config.
  3. POST one tiny chat completion, trying each route in order (next on 404):
       a. {host}/ai-gateway/mlflow/v1/chat/completions   — Unity AI Gateway,
          the route that serves three-part system.ai.* identifiers
       b. {host}/serving-endpoints/chat/completions       — OpenAI-compatible
       c. {host}/serving-endpoints/{model}/invocations    — legacy per-endpoint
  4. Assert HTTP 200 and print the model id and route that worked.

Every 404 body is printed — which routes reject three-part identifiers on a
given workspace is experiment data, not noise.

The model MUST be a three-part Unity Catalog identifier (catalog.schema.model),
e.g. system.ai.claude-sonnet-4-6. Legacy single-string endpoint
names are refused.
"""

import configparser
import os
import platform
import subprocess
import sys
from pathlib import Path

import requests

EXPERIMENT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MODEL = "system.ai.claude-sonnet-4-6"
REQUEST_TIMEOUT_SECONDS = 60


def fail(message: str) -> "None":
    print(f"❌ {message}", file=sys.stderr)
    sys.exit(1)


def get_token() -> str:
    """Mint a short-lived OAuth token via the bin/ credential helper."""
    if platform.system() == "Windows":
        helper = EXPERIMENT_ROOT / "bin" / "get-databricks-token.ps1"
        cmd = ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(helper)]
    else:
        helper = EXPERIMENT_ROOT / "bin" / "get-databricks-token.sh"
        cmd = ["sh", str(helper)]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        fail(f"credential helper failed (exit {result.returncode}):\n{result.stderr.strip()}")

    token = result.stdout.strip()
    if not token:
        fail("credential helper printed no token.")
    if token.startswith("dapi"):
        fail(
            "credential helper returned what looks like a static PAT (dapi...). "
            "This experiment is SSO-only — refusing to proceed."
        )
    return token


def get_host() -> str:
    """Resolve the workspace host from env, falling back to the CLI profile."""
    host = os.environ.get("DATABRICKS_HOST", "").strip()
    if not host:
        profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
        cfg_path = Path(os.environ.get("DATABRICKS_CONFIG_FILE", Path.home() / ".databrickscfg"))
        config = configparser.ConfigParser()
        config.read(cfg_path)
        if config.has_option(profile, "host"):
            host = config.get(profile, "host").strip()
        if not host:
            fail(
                f"could not resolve a workspace host: DATABRICKS_HOST is unset and "
                f"profile '{profile}' in {cfg_path} has no host. "
                f"Run: databricks auth login --host https://<your-workspace> --profile {profile}"
            )
    if not host.startswith("https://"):
        host = f"https://{host}"
    return host.rstrip("/")


def get_model() -> str:
    """Read RPW_MODEL and require a three-part Unity Catalog identifier."""
    model = os.environ.get("RPW_MODEL", DEFAULT_MODEL).strip()
    parts = model.split(".")
    if len(parts) != 3 or not all(parts):
        fail(
            f"RPW_MODEL='{model}' is not a three-part Unity Catalog identifier "
            f"(catalog.schema.model). Legacy single-string endpoint names like "
            f"'databricks-claude-sonnet-4-6' are refused — use e.g. '{DEFAULT_MODEL}'."
        )
    return model


def main() -> int:
    model = get_model()
    host = get_host()
    token = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    messages = [{"role": "user", "content": "Reply with the single word: ok"}]

    # (url, include model in payload) — next route is tried only on 404.
    routes = [
        (f"{host}/ai-gateway/mlflow/v1/chat/completions", True),
        (f"{host}/serving-endpoints/chat/completions", True),
        (f"{host}/serving-endpoints/{model}/invocations", False),
    ]

    response = None
    url = ""
    for url, include_model in routes:
        payload = {"messages": messages, "max_tokens": 16}
        if include_model:
            payload["model"] = model
        print(f"POST {url}  (model={model})")
        response = requests.post(
            url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT_SECONDS
        )
        if response.status_code != 404:
            break
        print(f"  → 404; body: {response.text.strip()}")

    if response.status_code == 200:
        print(f"✅ HTTP 200 via {url} — SSO OAuth auth verified end-to-end against model '{model}'.")
        return 0

    print(f"❌ HTTP {response.status_code} from {url}", file=sys.stderr)
    print(response.text, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
