# DuckDB + Unity Catalog Experiment

**Date:** 2026-03-11

This experiment explores how DuckDB can interact with Databricks Unity Catalog using its built-in catalog integrations. It tests read/write capabilities across UC connection types and table formats to understand what's actually supported versus what fails silently or throws errors.

**Workspace:** fe-vm-fe-randy-pitcher-workspace (AWS, VDM managed storage)
**DuckDB:** 1.5.0 | **Extensions:** iceberg, uc_catalog, delta, httpfs

---

## Results

> **Note:** Foreign Iceberg tables are excluded from all tests — complex setup, low priority for this experiment. 🚫

### Iceberg REST Connection (`iceberg` extension)

Delta tables require UniForm (`delta.universalFormat.enabledFormats = 'iceberg'`) to be visible via this path.

| Table Type | Read | Predicate Pushdown | Write (Append) | Write (Update) | Delete Row | Create Table | Drop Table |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Managed Delta** (UniForm) | ✅ | ✅ | ✅ | ✅ | ✅ | — | — |
| **External Delta** (UniForm) | ✅ | ✅ | ✅ | ✅ | ✅ | — | — |
| **Managed Iceberg** (native) | ❌¹ | ❌¹ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Foreign Iceberg** | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 |

> ¹ Native Iceberg tables on VDM managed storage fail with S3 403 — credential vending returns empty `storage-credentials`. Delta+UniForm tables vend credentials correctly. This appears to be a Databricks-side gap in how VDM storage credentials are vended for native Iceberg vs UniForm tables.

### UC REST Connection (`uc_catalog` extension, Delta protocol)

Requires **unnamed secret** workaround — named secrets are ignored due to [duckdb/unity_catalog#48](https://github.com/duckdb/unity_catalog/issues/48).
Requires `GRANT EXTERNAL USE SCHEMA` on the target schema.

| Table Type | Read | Predicate Pushdown | Write (Append) | Write (Update) | Delete Row | Create Table | Drop Table |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Managed Delta** (UniForm) | ❌² | ❌² | ❌² | ❌² | ❌² | ❌² | ❌² |
| **External Delta** (UniForm) | ❌² | ❌² | ❌² | ❌² | ❌² | ❌² | ❌² |
| **Managed Iceberg** (native) | ✅ | ✅ | ❌³ | ❌⁴ | ❌⁴ | — | — |
| **Foreign Iceberg** | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 |

> ² Delta+UniForm tables fail with "Bad Request" on the `temporary-table-credentials` API — the `uc_catalog` extension may not handle UniForm tables correctly
> ³ DeltaKernel error: "Unsupported: Unknown feature 'icebergWriterCompatV1'" — the delta extension can't write to native Iceberg tables
> ⁴ "Can only update/delete from base table" — DuckDB's delta extension doesn't support DML on Iceberg tables

### Delta Sharing Protocol (no native DuckDB client)

| Table Type | Read | Predicate Pushdown | Write (Append) | Write (Update) | Delete Row | Create Table | Drop Table |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **All types** | ❌⁵ | ❌⁵ | ❌⁶ | ❌⁶ | ❌⁶ | ❌⁶ | ❌⁶ |

> ⁵ DuckDB has no native Delta Sharing client — reads require a Python bridge (delta-sharing lib → pandas → DuckDB)
> ⁶ Delta Sharing protocol is read-only by design — no write endpoints exist in the spec

---

**Legend:** ✅ works &nbsp;|&nbsp; ❌ fails &nbsp;|&nbsp; ⚠️ partial &nbsp;|&nbsp; 🚫 out of scope &nbsp;|&nbsp; — not applicable

## Key Findings

### What works

- **Delta+UniForm via Iceberg REST = full CRUD** — Read, predicate pushdown, insert, update, delete all work. This is the recommended path for DuckDB ↔ UC integration.
- **Native Iceberg via UC Catalog = read-only** — Read and predicate pushdown work via the `uc_catalog` extension. Writes fail due to DeltaKernel incompatibility.
- **Iceberg REST Create/Drop Table** — DuckDB can create and drop Iceberg tables through the Iceberg REST catalog.
- **OAuth token auth** — Databricks SDK OAuth tokens work for both paths (no PAT required).

### What doesn't work (and why)

1. **Native Iceberg reads via Iceberg REST** — Credential vending returns empty `storage-credentials` for native Iceberg tables on VDM managed storage. Delta+UniForm tables vend correctly. This is a Databricks-side gap.

2. **Delta+UniForm via UC Catalog** — The `temporary-table-credentials` API returns "Bad Request" for Delta+UniForm tables. The `uc_catalog` extension may not handle UniForm table properties correctly.

3. **`uc_catalog` named secret bug** — Named secrets are silently ignored ([#48](https://github.com/duckdb/unity_catalog/issues/48)). The extension only looks for `__default_uc`. **Workaround: use unnamed `CREATE SECRET`.**

4. **`EXTERNAL USE SCHEMA` required** — Both paths require `GRANT EXTERNAL USE SCHEMA ON SCHEMA <schema> TO <user>` before credential vending works.

5. **Delta Sharing** — No native DuckDB client. Read-only by protocol design.

### Recommendations

- **Use Iceberg REST with Delta+UniForm tables** for full CRUD from DuckDB with UC governance
- Enable UniForm on all Delta tables you want DuckDB to access
- Grant `EXTERNAL USE SCHEMA` on target schemas
- Use unnamed secrets with `uc_catalog` extension until [#48](https://github.com/duckdb/unity_catalog/issues/48) is fixed

## Setup & Prerequisites

- [uv](https://docs.astral.sh/uv/) (Python package manager)
- Databricks CLI configured (`~/.databrickscfg` with `databricks-cli` auth type)
- DuckDB >= 1.4.0 (installed via `uv sync`)
- `GRANT EXTERNAL USE SCHEMA` on target schema

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

## Running

```bash
uv sync
uv run python scripts/00_setup_tables.py    # Create test tables
uv run python scripts/01_iceberg_rest.py     # Test Iceberg REST path
uv run python scripts/02_delta_uc_rest.py    # Test Delta / UC REST path
```

## DuckDB Connection Examples

### Iceberg REST (recommended for Delta+UniForm)

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
