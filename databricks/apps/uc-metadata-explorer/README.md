# UC Metadata Command Space (Databricks Apps MVP)

MVP Databricks App for reviewing Unity Catalog metadata quality and staging AI-generated metadata updates.

## What This MVP Delivers

- UC-style metadata explorer across catalogs, schemas, and objects.
- Gap scoring for table/column descriptions, owners, and tags.
- On-behalf-of-user (OBO) access model for Unity Catalog metadata reads.
- AI proposal generation for missing metadata:
  - Uses Databricks Foundation Model endpoint when configured.
  - Falls back to deterministic heuristics when model endpoint is unavailable.
- Human review and selective confirmation before persistence.
- Persistence to Lakebase Postgres only (no write-back sync to Unity Catalog).

## Architecture

- **Backend**: FastAPI (`app.py`, `server/`)
  - Queries `system.information_schema` with Databricks SQL Connector.
  - Reads OBO token from `x-forwarded-access-token`.
  - Saves confirmed proposals to Lakebase in `metadata_review.proposals`.
- **Frontend**: React + Vite (`frontend/`)
  - Split-pane UI inspired by Unity Catalog explorer patterns.
  - Object list with gap score badges.
  - Detail panel for table/column metadata.
  - Proposal review/edit/confirm workflow.

## Core Endpoints

- `GET /api/v1/healthcheck`
- `GET /api/v1/metadata/objects`
- `GET /api/v1/metadata/objects/{catalog}/{schema}/{object}`
- `POST /api/v1/proposals/generate`
- `POST /api/v1/proposals/confirm`

## Local Development

### 1) Backend

From project root:

```bash
uv sync
uv run uvicorn app:app --reload --port 8000
```

### 2) Frontend

```bash
cd frontend
npm install
npm run dev
```

The Vite dev server proxies `/api/*` to `http://127.0.0.1:8000`.

### 3) Required Environment

Environment file preference in this project:

- `template.env` is committed and defines expected structure.
- `dev.env`, `test.env`, and `prod.env` are local-only and ignored by git.
- Active file is selected by `APP_ENV` (defaults to `dev`).

Quick setup:

```bash
cp template.env dev.env
cp template.env test.env
cp template.env prod.env
```

Run with a specific env file:

```bash
APP_ENV=dev uv run uvicorn app:app --reload --port 8000
```

For metadata queries:

- `DATABRICKS_HOST` (optional if already configured via Databricks CLI profile)
- `DATABRICKS_SQL_WAREHOUSE_ID` (or `WAREHOUSE_ID`)
- Auth available through:
  - Databricks Apps OBO header (`x-forwarded-access-token`), or
  - local bearer header / Databricks CLI auth fallback.

For Lakebase persistence:

- `PGHOST`
- `PGPORT`
- `PGDATABASE`
- `PGUSER`

Optional AI model endpoint:

- `DATABRICKS_SERVING_ENDPOINT` (defaults in `app.yaml` to `databricks-claude-sonnet-4-5`)

### 4) Optional local SQLite metadata cache (faster dev loop)

You can snapshot metadata from `system.information_schema` into a local SQLite file and develop against that cache:

```bash
APP_ENV=dev uv run python scripts/build-sqlite-cache.py --table-limit 3000
```

Default output:

- `.cache/uc_metadata_cache.db`

Optional flags:

- `--catalogs main,prod` to scope snapshot
- `--out /custom/path/cache.db` to change output file

Use cached source via API:

```bash
GET /api/v1/metadata/objects?source=cache
GET /api/v1/metadata/objects/{catalog}/{schema}/{object}?source=cache
```

In the UI, enable **Use local SQLite cache (faster dev loop)** in the filters panel.

## Deploy to Databricks Apps

1. Build frontend for static serving:

```bash
cd frontend
npm run build
```

2. Create and deploy app:

```bash
databricks apps create uc-metadata-command-space
databricks apps deploy uc-metadata-command-space --source-code-path .
```

3. Add app resources:

- SQL Warehouse resource (`Can use`) with key `sql_warehouse`.
- Lakebase Database resource (`Can connect and create`) with key `database`.
- (Optional) Model serving endpoint resource for proposal generation.

4. Enable **User authorization (OBO)** and include scopes needed for SQL/model use.

## Data Model in Lakebase

Table: `metadata_review.proposals`

- `catalog_name`, `schema_name`, `object_name`, `object_type`
- `created_by`
- `reviewer_notes`
- `payload` (JSON with selected changes)
- `created_at`

## Reference Implementations and Docs

- [Databricks Apps auth (OBO)](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth)
- [Databricks Apps env variables and resources](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/environment-variables)
- [Databricks Apps Lakebase resource](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/lakebase)
- [Databricks Apps FastAPI cookbook](https://apps-cookbook.dev/docs/fastapi/getting_started/create/)
- [Databricks Apps OBO cookbook snippet](https://apps-cookbook.dev/docs/reflex/authentication/users_obo/)
- [Unity Catalog information schema docs](https://docs.databricks.com/en/sql/language-manual/sql-ref-information-schema.html)

## Lakebase Infrastructure via Databricks Asset Bundles

This repo now includes a bundle configuration for Lakebase project/branch/endpoint infrastructure:

- `databricks.yml`
- `bundle/resources/lakebase.yml`

### Dev branch naming from git branch

The dev Lakebase branch ID is generated as:

- `dev-<sanitized-git-branch>`

Sanitization is handled by `scripts/git-branch-slug.sh` and enforces Lakebase branch ID constraints:

- lowercase only
- non `[a-z0-9]` replaced with `-`
- starts with a letter (prefixes `b-` when needed)
- max 63 chars total for `dev-<slug>`

### Validate and deploy

From project root:

```bash
./scripts/dab-lakebase-validate.sh
./scripts/dab-lakebase-deploy.sh
```

To deploy for a specific branch name without checking out:

```bash
./scripts/dab-lakebase-deploy.sh feature/my-branch
```

### Get PGHOST / PGDATABASE / PGUSER

After deploy, print export commands for the active git branch's endpoint:

```bash
./scripts/dab-lakebase-pg-env.sh
```

Then apply to shell:

```bash
eval "$(./scripts/dab-lakebase-pg-env.sh)"
```

Optional overrides:

- `LAKEBASE_PROJECT_ID` (default `uc-metadata`)
- `LAKEBASE_ENDPOINT_ID` (default `app-rw`)
