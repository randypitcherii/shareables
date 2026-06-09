import html
import json
import os
import re
import shutil
import threading
import time
from collections import Counter
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from databricks.sdk import WorkspaceClient


DEFAULT_ARTIFACT_CATALOG = "fe_randy_pitcher_workspace_catalog"
DEFAULT_ARTIFACT_SCHEMA = "dbt_artifacts"
DEFAULT_ARTIFACT_VOLUME = "dbt_demo_artifacts"
DEPLOYMENT_ENVIRONMENT = os.environ.get("DBT_DEPLOYMENT_ENVIRONMENT", "production")
ARTIFACT_VOLUME_FULL_NAME = os.environ.get("DBT_ARTIFACT_VOLUME_FULL_NAME", "")
ARTIFACT_CATALOG = os.environ.get("DBT_ARTIFACT_CATALOG", DEFAULT_ARTIFACT_CATALOG)
ARTIFACT_SCHEMA = os.environ.get("DBT_ARTIFACT_SCHEMA", DEFAULT_ARTIFACT_SCHEMA)
ARTIFACT_VOLUME = os.environ.get("DBT_ARTIFACT_VOLUME", DEFAULT_ARTIFACT_VOLUME)
LOCAL_DOCS_PATH = Path(os.environ.get("LOCAL_DBT_DOCS_PATH", "/tmp/dbt_docs")).resolve()
LOCAL_STATE_PATH = Path(os.environ.get("LOCAL_DBT_STATE_PATH", "/tmp/dbt_state")).resolve()
PRODUCTION_JOB_ID = os.environ.get("DBT_PRODUCTION_JOB_ID", "")
SYNC_INTERVAL_SECONDS = int(os.environ.get("DBT_DOCS_SYNC_INTERVAL_SECONDS", "30"))
LOG_TAIL_LINES = int(os.environ.get("DBT_RUN_LOG_TAIL_LINES", "160"))

_sync_lock = threading.Lock()
_last_sync_at = 0.0
_last_sync_error: str | None = None
_ansi_escape = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def _artifact_volume_path() -> str:
    full_name = ARTIFACT_VOLUME_FULL_NAME.strip()
    if full_name:
        if full_name.startswith("/Volumes/"):
            return full_name.rstrip("/")

        parts = full_name.split(".")
        if len(parts) != 3:
            raise ValueError(
                "DBT_ARTIFACT_VOLUME_FULL_NAME must be /Volumes/catalog/schema/volume "
                "or catalog.schema.volume, "
                f"got {full_name!r}"
            )
        catalog, schema, volume = parts
    else:
        catalog, schema, volume = ARTIFACT_CATALOG, ARTIFACT_SCHEMA, ARTIFACT_VOLUME

    return f"/Volumes/{catalog}/{schema}/{volume}"


REMOTE_ARTIFACT_VOLUME_PATH = _artifact_volume_path()
REMOTE_DOCS_PATH = os.environ.get("DBT_DOCS_PATH", f"{REMOTE_ARTIFACT_VOLUME_PATH}/docs/latest").rstrip("/")
REMOTE_STATE_PATH = os.environ.get("DBT_STATE_PATH", f"{REMOTE_ARTIFACT_VOLUME_PATH}/state/latest").rstrip("/")


def _download_file(client: WorkspaceClient, remote_path: str, local_path: Path):
    response = client.files.download(remote_path)
    if response.contents is None:
        raise FileNotFoundError(remote_path)

    local_path.parent.mkdir(parents=True, exist_ok=True)

    with local_path.open("wb") as file:
        shutil.copyfileobj(response.contents, file)

    response.contents.close()


def _sync_directory(client: WorkspaceClient, remote_dir: str, local_dir: Path):
    local_dir.mkdir(parents=True, exist_ok=True)

    for entry in client.files.list_directory_contents(remote_dir):
        if not entry.path or not entry.name:
            continue

        target = local_dir / entry.name
        if entry.is_directory:
            _sync_directory(client, entry.path, target)
        else:
            _download_file(client, entry.path, target)


def _download_state(client: WorkspaceClient):
    LOCAL_STATE_PATH.mkdir(parents=True, exist_ok=True)
    _download_file(client, f"{REMOTE_STATE_PATH}/run_results.json", LOCAL_STATE_PATH / "run_results.json")


def _load_json(path: Path):
    if not path.exists():
        return None

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def _format_iso_timestamp(value: str | None) -> str:
    if not value:
        return "Unknown"

    try:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return value

    return parsed.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _format_epoch_millis(value: int | None) -> str:
    if not value:
        return "Unknown"

    return datetime.fromtimestamp(value / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _enum_value(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "value"):
        return value.value
    return str(value)


def _status_label(status: str | None) -> str:
    normalized = (status or "Unknown").upper()
    if normalized == "SUCCESS":
        return "✅ Success"
    if normalized in {"SUCCESS_WITH_WARNINGS", "WARN", "WARNING"}:
        return "⚠️ Succeeded with Warnings"
    if "FAIL" in normalized or "ERROR" in normalized:
        return "❌ Failure"
    return status or "Unknown"


def _status_needs_logs(status: str | None) -> bool:
    normalized = (status or "").upper()
    return "WARN" in normalized or "FAIL" in normalized or "ERROR" in normalized


def _clean_logs(logs: str) -> str:
    cleaned = _ansi_escape.sub("", logs).replace("```", "'''")
    lines = cleaned.splitlines()
    if len(lines) > LOG_TAIL_LINES:
        lines = [f"... trimmed to last {LOG_TAIL_LINES} lines ...", *lines[-LOG_TAIL_LINES:]]
    return "\n".join(lines).strip()


def _task_run_logs(client: WorkspaceClient, run, task_key: str) -> str:
    task_run_id = None
    for task in run.tasks or []:
        if task.task_key == task_key:
            task_run_id = task.run_id
            break

    if not task_run_id:
        return ""

    try:
        output = client.jobs.get_run_output(task_run_id)
    except Exception as exc:
        return f"Unable to fetch run logs for task {task_key}: {exc}"

    logs = output.logs or output.error or ""
    if output.logs_truncated:
        logs = f"{logs}\n\n[Logs were truncated by Databricks.]"
    return _clean_logs(logs)


def _latest_run_summary(client: WorkspaceClient):
    if not PRODUCTION_JOB_ID:
        return {
            "status": "Unknown",
            "message": "DBT_PRODUCTION_JOB_ID is not configured.",
            "updated_at": "Unknown",
            "url": "",
            "logs": "",
        }

    runs = list(client.jobs.list_runs(job_id=int(PRODUCTION_JOB_ID), limit=1, expand_tasks=True))
    if not runs:
        return {
            "status": "No runs found",
            "message": "The production dbt workflow has not run yet.",
            "updated_at": "Unknown",
            "url": "",
            "logs": "",
        }

    run = runs[0]
    state = run.state
    result_state = _enum_value(getattr(state, "result_state", None))
    life_cycle_state = _enum_value(getattr(state, "life_cycle_state", None))
    state_message = getattr(state, "state_message", "") if state else ""
    status = result_state or life_cycle_state or "Unknown"
    updated_at = _format_epoch_millis(run.end_time or run.start_time)

    return {
        "status": status,
        "message": state_message or "",
        "updated_at": updated_at,
        "url": run.run_page_url or "",
        "logs": _task_run_logs(client, run, "dbt_build_hourly"),
    }


def _state_summary():
    run_results = _load_json(LOCAL_STATE_PATH / "run_results.json")
    if not run_results:
        return {
            "generated_at": "Unknown",
            "status": "Unknown",
            "status_counts": {},
            "failures": [],
        }

    results = run_results.get("results", [])
    status_counts = Counter(result.get("status", "unknown") for result in results)
    failures = [
        result.get("unique_id", "unknown")
        for result in results
        if result.get("status") not in {"success", "pass", "skipped", "warn"}
    ]
    status = "FAILED" if failures else "SUCCESS_WITH_WARNINGS" if status_counts.get("warn") else "SUCCESS"

    return {
        "generated_at": _format_iso_timestamp(run_results.get("metadata", {}).get("generated_at")),
        "status": status,
        "status_counts": dict(sorted(status_counts.items())),
        "failures": failures[:5],
    }


def _docs_generated_at(manifest):
    manifest_generated_at = (manifest or {}).get("metadata", {}).get("generated_at")
    docs_run_results = _load_json(LOCAL_DOCS_PATH / "run_results.json")
    docs_generated_at = (docs_run_results or {}).get("metadata", {}).get("generated_at")
    return _format_iso_timestamp(docs_generated_at or manifest_generated_at)


def _render_overview(manifest, run_summary):
    state = _state_summary()
    escaped_run_url = html.escape(run_summary["url"], quote=True)
    run_link_markdown = (
        f'<a href="{escaped_run_url}" target="_blank" rel="noopener noreferrer">Open latest Databricks run</a>'
        if run_summary["url"]
        else "Unavailable"
    )
    status_counts = ", ".join(f"{name}: {count}" for name, count in state["status_counts"].items()) or "Unknown"
    failure_lines = "\n".join(f"- `{failure}`" for failure in state["failures"]) or "- None"
    workflow_status = _status_label(run_summary["status"])
    build_status = _status_label(state["status"])
    should_show_logs = _status_needs_logs(run_summary["status"]) or _status_needs_logs(state["status"])
    run_logs_section = ""
    if should_show_logs:
        logs = run_summary["logs"] or "No run logs were available."
        run_logs_section = f"""
## Latest dbt Build Logs

```text
{logs}
```
"""

    return f"""# rpw-dbt-databricks-reference

This documentation is served from production dbt artifacts captured in a Unity Catalog volume.

## Production Run Metadata

| Field | Value |
| --- | --- |
| Deployment environment | `{DEPLOYMENT_ENVIRONMENT}` |
| Last updated at | `{_docs_generated_at(manifest)}` |
| Latest state captured at | `{state["generated_at"]}` |
| Latest workflow run status | `{workflow_status}` |
| Build state status | `{build_status}` |
| Build result counts | `{status_counts}` |
| Latest workflow run | {run_link_markdown} |

Run message: `{run_summary["message"] or "None"}`

## Current Build Failures

{failure_lines}

{run_logs_section}

## Artifact Locations

- Docs: `{REMOTE_DOCS_PATH}`
- State: `{REMOTE_STATE_PATH}`
"""


def _patch_manifest_overview(client: WorkspaceClient):
    manifest_path = LOCAL_DOCS_PATH / "manifest.json"
    manifest = _load_json(manifest_path)
    if not manifest:
        return

    overview = _render_overview(manifest, _latest_run_summary(client))
    project_name = manifest.get("metadata", {}).get("project_name", "rpw_dbt_databricks_reference")
    docs = manifest.setdefault("docs", {})

    for package_name in {"dbt", project_name}:
        unique_id = f"doc.{package_name}.__overview__"
        docs[unique_id] = {
            "block_contents": overview,
            "name": "__overview__",
            "original_file_path": "models/docs_homepage.md",
            "package_name": package_name,
            "path": "models/docs_homepage.md",
            "resource_type": "doc",
            "unique_id": unique_id,
        }

    with manifest_path.open("w", encoding="utf-8") as file:
        json.dump(manifest, file, separators=(",", ":"))


def sync_docs_if_needed(force: bool = False):
    global _last_sync_at, _last_sync_error

    now = time.time()
    index_path = LOCAL_DOCS_PATH / "index.html"
    if not force and index_path.exists() and now - _last_sync_at < SYNC_INTERVAL_SECONDS:
        return

    with _sync_lock:
        now = time.time()
        if not force and index_path.exists() and now - _last_sync_at < SYNC_INTERVAL_SECONDS:
            return

        try:
            staging_path = LOCAL_DOCS_PATH.with_name(f"{LOCAL_DOCS_PATH.name}.staging")
            if staging_path.exists():
                shutil.rmtree(staging_path)

            client = WorkspaceClient()
            _sync_directory(client, REMOTE_DOCS_PATH, staging_path)
            _download_state(client)

            if not (staging_path / "index.html").exists():
                raise FileNotFoundError(f"{REMOTE_DOCS_PATH}/index.html")

            if LOCAL_DOCS_PATH.exists():
                shutil.rmtree(LOCAL_DOCS_PATH)
            staging_path.replace(LOCAL_DOCS_PATH)
            _patch_manifest_overview(client)

            _last_sync_at = time.time()
            _last_sync_error = None
            print(
                f"Synced dbt docs from {REMOTE_DOCS_PATH} to {LOCAL_DOCS_PATH} "
                f"and state from {REMOTE_STATE_PATH} to {LOCAL_STATE_PATH}",
                flush=True,
            )
        except Exception as exc:
            _last_sync_error = str(exc)
            print(f"Failed to sync dbt docs from {REMOTE_DOCS_PATH}: {exc}", flush=True)


def _should_sync_request(path: str) -> bool:
    request_path = urlsplit(path).path
    return request_path in {
        "/",
        "/index.html",
        "/manifest.json",
        "/catalog.json",
        "/run_results.json",
        "/semantic_manifest.json",
        "/graph_summary.json",
    }


class DbtDocsHandler(SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(LOCAL_DOCS_PATH), **kwargs)

    def do_GET(self):
        if _should_sync_request(self.path):
            sync_docs_if_needed()

        if self.path in {"/", "/index.html"} and not (LOCAL_DOCS_PATH / "index.html").exists():
            self._write_missing_docs_page()
            return

        super().do_GET()

    def do_HEAD(self):
        if _should_sync_request(self.path):
            sync_docs_if_needed()
        super().do_HEAD()

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def _write_missing_docs_page(self):
        remote_docs_path = html.escape(REMOTE_DOCS_PATH)
        remote_state_path = html.escape(REMOTE_STATE_PATH)
        local_docs_path = html.escape(str(LOCAL_DOCS_PATH))
        sync_error = html.escape(_last_sync_error or "No sync has completed yet.")
        body = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>dbt docs not generated yet</title>
    <style>
      body {{
        color: #111827;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        margin: 3rem auto;
        max-width: 48rem;
        padding: 0 1.5rem;
      }}
      code {{
        background: #f3f4f6;
        border-radius: 0.25rem;
        padding: 0.15rem 0.3rem;
      }}
    </style>
  </head>
  <body>
    <h1>dbt docs are not available yet</h1>
    <p>Run the <code>rpw-dbt-databricks-reference Production Daily</code> workflow to generate docs.</p>
    <p>Remote docs path: <code>{remote_docs_path}</code></p>
    <p>Remote state path: <code>{remote_state_path}</code></p>
    <p>Local cache path: <code>{local_docs_path}</code></p>
    <p>Last sync error: <code>{sync_error}</code></p>
  </body>
</html>
"""
        encoded = body.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def main():
    sync_docs_if_needed(force=True)

    port = int(os.environ.get("DATABRICKS_APP_PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), DbtDocsHandler)
    print(f"Serving dbt docs from {LOCAL_DOCS_PATH} on 0.0.0.0:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
