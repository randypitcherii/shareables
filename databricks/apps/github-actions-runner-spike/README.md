# Randy Databricks Apps Starter

Canonical starter template for rapid Databricks Apps iteration in a sandbox workspace.

## Current MVP

- FastAPI service with `uv sync --frozen` launch (no pip, no requirements.txt).
- **Endpoints**:
  - `GET /` — interactive terminal UI (`static/terminal.html`)
  - `GET /api/v1/healthcheck` — app health
  - `GET /api/v1/db/health` — Lakebase connectivity check
  - `POST /api/v1/shell/run` — execute shell command (JSON response)
  - `POST /api/v1/shell/stream` — execute shell command (streaming text)
  - `POST /api/v1/shell/complete` — tab completion candidates
  - `GET /api/v1/auth/context` — OBO forwarded user/token presence
- `db.py` module with `get_connection()` helper using M2M OAuth token auth
- Per-session shell cwd persisted in local SQLite (`SESSION_STATE_DB_PATH`) for multi-worker consistency.
- Root `Makefile` and `uv` workflow.
- DAB + Lakebase template defaults (dev branch, prod main).

### Prerequisites

- Python >= 3.10
- [uv](https://docs.astral.sh/uv/) (pre-installed on Databricks Apps runtime)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html)

## Quick Start

```bash
make dev
```

Local review is intentionally pinned to `http://127.0.0.1:8000/` (canonical port).  
`make dev` automatically stops any prior process listening on that port before starting a fresh instance.

Smoke test:

```bash
curl -s http://127.0.0.1:8000/api/v1/healthcheck
curl -s -X POST http://127.0.0.1:8000/api/v1/shell/run \
  -H "content-type: application/json" \
  -d '{"argv":["bash","-lc","echo hello from sandbox"]}'
```

Open the manual UI at [http://127.0.0.1:8000/](http://127.0.0.1:8000/).
Check the **Runtime marker** badge near the page title to confirm you are on the latest run.

Session-state storage defaults to `/tmp/randy_apps_starter_session_state.sqlite3` (override with `SESSION_STATE_DB_PATH`).

Run tests:

```bash
make test
```

## Deploy with Databricks Asset Bundles

```bash
make verify
make deploy-dev
```

`make deploy-dev` runs both bundle deployment and explicit app source deployment so the app reaches an active deployment state.
It deploys source from the bundle workspace path (`/Workspace/Users/<user>/.bundle/randy_apps_starter/dev/files`), not the local filesystem path.

Production deploy:

```bash
make deploy-prod
```

## DAB and Lakebase Standards

- **Dev resource prefixing**: `make verify` / `make deploy-dev` compute a slug-safe user prefix (`[a-z0-9-]`, starts with letter). App names are capped to 30 chars; Lakebase project IDs are capped to 63 chars.
- **Prod resource names**: no user prefix, canonical IDs.
- **Lakebase strategy**:
  - dev uses `dev-<git-branch-slug>`
  - prod uses `main`
  - no branch merge automation in this template

Branch slug generation:

```bash
./scripts/git-branch-slug.sh
```

Override at deploy time if needed:

```bash
databricks bundle deploy -t dev --var "git_branch_slug=my-feature"
```

Override dev names explicitly if needed:

```bash
databricks bundle deploy -t dev \
  --var "app_name=my-prefix-randy-apps-starter" \
  --var "lakebase_project_id=my-prefix-randy-apps-starter"
```

## Lakebase (Postgres) Connectivity

The app connects to its attached Lakebase database using M2M OAuth tokens.
The Databricks Apps runtime auto-injects `PGHOST`, `PGPORT`, `PGDATABASE`, and `PGSSLMODE`
when a Lakebase database resource is configured in `databricks.yml`.

Authentication uses the app's service principal credentials (`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`)
to fetch an OAuth token from the workspace OIDC endpoint. Tokens are cached and auto-refreshed.

```python
from db import get_connection, check_connectivity

# Get a raw psycopg2 connection
conn = get_connection()
cur = conn.cursor()
cur.execute("SELECT 1")
conn.close()

# Or run a health check
result = check_connectivity()  # {"ok": True, "latency_ms": ..., ...}
```

Key details:
- **User**: `DATABRICKS_CLIENT_ID` (service principal UUID)
- **Password**: M2M OAuth access token (not the client secret directly)
- **Schema**: The service principal cannot write to `public` — create your own schema
- **Endpoint**: `GET /api/v1/db/health` returns live connectivity status

## OBO Integration

- Endpoint `GET /api/v1/auth/context` reads `x-forwarded-user` and token presence from `x-forwarded-access-token`.
- The endpoint intentionally reports only token presence (never token value).

## Shell API

Request body for `shell/run` and `shell/stream`:

```json
{
  "argv": ["bash", "-lc", "echo hello"],
  "session_id": "optional-session-id",
  "timeout_seconds": 20
}
```

- `session_id` persists working directory across requests (SQLite-backed)
- Default timeout: 20 seconds (max 120)

Request body for `shell/complete`:

```json
{"line": "partial-inp", "session_id": "optional"}
```

Returns `{input, fragment, common_prefix, completed_input, candidates}`.

## Dependency Management

Dependencies are managed by `uv` via `pyproject.toml` + `uv.lock`. The `app.yaml` command runs
`uv sync --frozen` before launching uvicorn, so no `pip install` step is needed.

To update dependencies locally:

```bash
uv add <package>        # adds to pyproject.toml and updates uv.lock
uv lock                 # regenerate lockfile after manual pyproject.toml edits
```

## Notes

- This MVP intentionally prioritizes exploration speed over hardening.
- The shell sandbox has full filesystem and env access — do not expose to untrusted users.
- Security hardening and default zerobus logging are tracked for follow-up.
- Local review safety rails:
  - canonical review port: `8000`
  - `make dev` replaces any previous local reviewer process on that port
  - runtime marker is exposed in UI and `GET /api/v1/healthcheck` for quick freshness checks
