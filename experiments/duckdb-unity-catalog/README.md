# DuckDB + Unity Catalog Experiment

**Date:** 2026-03-11

This experiment explores how DuckDB can interact with Databricks Unity Catalog using its built-in catalog integrations. It tests read/write capabilities across UC connection types and table formats to understand what's actually supported versus what fails silently or throws errors.

**Workspace:** `<your-workspace>` (set via `DATABRICKS_HOST` env var)
**DuckDB:** 1.5.0 | **Extensions:** iceberg, uc_catalog, delta, httpfs

---

## Results

> **Note:** Foreign Iceberg tables are excluded from all tests — complex setup, low priority for this experiment. 🚫

### Iceberg REST Connection (`iceberg` extension)

Delta tables require UniForm (`delta.universalFormat.enabledFormats = 'iceberg'`) to be visible via this path.

| Table Type | Read | Predicate Pushdown | Write (Append) | Write (Update) | Delete Row | Create Table | Drop Table |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Managed Delta** (UniForm) | ✅ | ✅ | ❌¹ | ❌¹ | ❌¹ | — | — |
| **External Delta** (UniForm) | ✅ | ✅ | ❌¹ | ❌¹ | ❌¹ | — | — |
| **Managed Iceberg** (native) | ❌² | ✅ | ❌² | ❌² | ❌² | ✅⁸ | ✅⁸ |
| **Foreign Iceberg** | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 |

> ¹ Delta+UniForm tables are read-only via Iceberg REST — Databricks does not support writes to Delta tables through the Iceberg REST API, even with UniForm enabled. Write attempts fail with S3 403.
> ² Native Iceberg tables on VDM managed storage fail with S3 403 — credential vending returns valid credentials (confirmed via raw API inspection), but DuckDB fails to use them for data file access. PyIceberg reads the same tables successfully. Create/Drop DDL works because it goes through the catalog API, not direct storage access.
> ⁸ CREATE/DROP TABLE via Iceberg REST works as catalog API operations. **However**, UC actually creates Delta tables (with UniForm/Iceberg compatibility) even when created through the Iceberg REST endpoint — verified via Databricks SDK `tables.get()` which reports `data_source_format=DELTA`.

### UC REST Connection (`uc_catalog` extension, Delta protocol)

Requires **unnamed secret** workaround — named secrets are ignored due to [duckdb/unity_catalog#48](https://github.com/duckdb/unity_catalog/issues/48).
Requires `GRANT EXTERNAL USE SCHEMA` on the target schema.

| Table Type | Read | Predicate Pushdown | Write (Append) | Write (Update) | Delete Row | Create Table | Drop Table |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Managed Delta** (UniForm) | ❌³ | ❌³ | ❌³ | ❌³ | ❌³ | ❌³ | ❌³ |
| **External Delta** (UniForm) | ❌³ | ❌³ | ❌³ | ❌³ | ❌³ | ❌³ | ❌³ |
| **Managed Iceberg** (native) | ✅ | ✅ | ❌⁴ | ❌⁵ | ❌⁵ | — | — |
| **Foreign Iceberg** | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 |

> **Verified** 2026-03-12 via `pytest tests/test_uc_rest.py` — 2 passed, 13 xfailed (all results match expectations)

> ³ Delta+UniForm tables fail with "Bad Request" on the `temporary-table-credentials` API
> ⁴ DeltaKernel error: "Unsupported: Unknown feature 'icebergWriterCompatV1'"
> ⁵ "Can only update/delete from base table"

### Delta Sharing Protocol (no native DuckDB client)

| Table Type | Read | Predicate Pushdown | Write (Append) | Write (Update) | Delete Row | Create Table | Drop Table |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **All types** | ❌⁶ | ❌⁶ | ❌⁷ | ❌⁷ | ❌⁷ | ❌⁷ | ❌⁷ |

> ⁶ No native DuckDB Delta Sharing client — requires Python bridge
> ⁷ Protocol is read-only by design

---

**Legend:** ✅ works &nbsp;|&nbsp; ❌ fails &nbsp;|&nbsp; ⚠️ partial &nbsp;|&nbsp; 🚫 out of scope &nbsp;|&nbsp; — not applicable

## Cross-Workspace Validation

Tested on 3 workspaces (2026-03-11):

| Path | fe-vm (AWS, VDM storage) | e2-demo-field-eng (AWS) | logfood (Azure) |
|---|---|---|---|
| Iceberg REST + Delta+UniForm | Read-only (writes fail 403) | Read-only (writes fail 403) | ADLS not supported |
| Iceberg REST + Native Iceberg | Reads fail, writes fail | Reads fail, writes fail | ADLS not supported |
| uc_catalog + Delta+UniForm | Bad Request | Reads work, writes fail | Permission denied |
| uc_catalog + Native Iceberg | Read-only | Read-only | Permission denied |

---

## Key Findings

### What works

- **Delta+UniForm via Iceberg REST = read-only** — Read and predicate pushdown work. This is the recommended path for DuckDB ↔ UC integration.
- **Native Iceberg via UC Catalog = read-only** — Read and predicate pushdown work via the `uc_catalog` extension. Writes fail due to DeltaKernel incompatibility.
- **Iceberg REST Create/Drop Table** — DuckDB can create and drop native Iceberg tables through the Iceberg REST catalog.
- **OAuth token auth** — Databricks SDK OAuth tokens work for both paths (no PAT required).

### What doesn't work (and why)

1. **ALL writes via both paths** — Every write operation fails with S3 403 or other errors. DuckDB + UC is currently read-only in practice.

2. **Native Iceberg reads via Iceberg REST** — DuckDB fails to use vended credentials for data file access (S3 403), even though credentials are valid (confirmed via raw API inspection). PyIceberg reads the same tables successfully. This is a DuckDB client-side issue.

3. **Delta+UniForm via UC Catalog** — The `temporary-table-credentials` API returns "Bad Request" for Delta+UniForm tables.

4. **`uc_catalog` named secret bug** — Named secrets are silently ignored ([#48](https://github.com/duckdb/unity_catalog/issues/48)). The extension only looks for `__default_uc`. **Workaround: use unnamed `CREATE SECRET`.**

5. **`EXTERNAL USE SCHEMA` required** — Both paths require `GRANT EXTERNAL USE SCHEMA ON SCHEMA <schema> TO <user>` before credential vending works.

6. **Delta Sharing** — No native DuckDB client. Read-only by protocol design.

### Recommendations

- **Use Iceberg REST with Delta+UniForm tables** for **read-only** access from DuckDB with UC governance
- **Use UC Catalog with native Iceberg tables** for **read-only** access as an alternative
- DuckDB + UC is currently **read-only** in practice — no write path works
- Enable UniForm on Delta tables you want DuckDB to read via Iceberg REST
- Grant `EXTERNAL USE SCHEMA` on target schemas
- Use unnamed secrets with `uc_catalog` extension until [#48](https://github.com/duckdb/unity_catalog/issues/48) is fixed

## Configuration

Copy `.env.template` to `.env` and fill in the required values:

```bash
cp .env.template .env
# Edit .env with your values
```

| Env Var | Required | Description |
|---|---|---|
| `DATABRICKS_HOST` | yes | Workspace hostname, e.g. `https://your-workspace.cloud.databricks.com` |
| `DATABRICKS_CATALOG` | yes | Unity Catalog catalog name to use for tests |
| `DATABRICKS_SCHEMA` | no | Schema name (default: `duckdb_uc_experiment`) |

The scripts use the Databricks SDK default auth chain (`~/.databrickscfg`, env vars, OAuth) — no separate token env var needed.

## Setup & Prerequisites

- [uv](https://docs.astral.sh/uv/) (Python package manager)
- Databricks CLI configured (`~/.databrickscfg` with `databricks-cli` auth type)
- DuckDB >= 1.4.0 (installed via `uv sync`)
- `GRANT EXTERNAL USE SCHEMA` on target schema
- Required env vars set (see Configuration above)

### Table setup

Delta tables are created with UniForm enabled. The Iceberg table is native `USING ICEBERG`.

```sql
-- Delta + UniForm
CREATE TABLE ... USING DELTA
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg', 'delta.enableIcebergCompatV2' = 'true')

-- Native Iceberg
CREATE TABLE ... USING ICEBERG
```

## Scripts

| Script | Description |
|--------|-------------|
| `scripts/00_setup_tables.py` | Creates test tables via Databricks Connect (serverless) |
| `scripts/01_iceberg_rest.py` | Tests all operations via Iceberg REST catalog endpoint |
| `scripts/02_delta_uc_rest.py` | Tests Delta Sharing REST + delta_scan direct storage paths |
| `scripts/iceberg_rest_eval.py` | **Isolated** Iceberg REST evaluation — self-contained, no shared imports, tests Reads/DML/DDL for Managed Iceberg + Managed Delta |
| `tests/test_iceberg_rest.py` | **Pytest** Iceberg REST evaluation — proper test suite with xfail markers, DDL verified via Databricks SDK |
| `tests/test_uc_rest.py` | **Pytest** UC REST evaluation — proper test suite with xfail markers for uc_catalog extension |

## Running

```bash
uv sync
uv run python scripts/00_setup_tables.py    # Create test tables
uv run python scripts/01_iceberg_rest.py     # Test Iceberg REST path
uv run python scripts/02_delta_uc_rest.py    # Test Delta / UC REST path
uv run pytest tests/test_iceberg_rest.py -v  # Pytest eval (recommended)
uv run pytest tests/test_uc_rest.py -v     # Pytest UC REST eval
```

## DuckDB Connection Examples

### Iceberg REST (recommended for Delta+UniForm, read-only)

```sql
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;

CREATE SECRET (
    TYPE iceberg,
    TOKEN '<oauth-or-pat-token>'
);

ATTACH '<catalog_name>' AS uc (
    TYPE iceberg,
    ENDPOINT 'https://<workspace>/api/2.1/unity-catalog/iceberg-rest'
);

SELECT * FROM uc.<schema>.<table>;
```

### UC Catalog (for native Iceberg read-only)

```sql
INSTALL uc_catalog FROM core; LOAD uc_catalog;
INSTALL delta; LOAD delta;

-- Must be unnamed (named secrets are ignored — bug #48)
CREATE SECRET (
    TYPE UC,
    TOKEN '<oauth-or-pat-token>',
    ENDPOINT 'https://<workspace>',
    AWS_REGION 'us-east-1'
);

ATTACH '<catalog_name>' AS uc (TYPE UC_CATALOG);

SELECT * FROM uc.<schema>.<table>;
```
