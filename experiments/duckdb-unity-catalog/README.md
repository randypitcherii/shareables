# DuckDB + Unity Catalog Experiment

**Date:** 2026-03-11

This experiment explores how DuckDB can interact with Databricks Unity Catalog using its built-in catalog integrations. It tests read/write capabilities across UC connection types and table formats to understand what's actually supported versus what fails silently or throws errors.

**Workspace:** fe-vm-fe-randy-pitcher-workspace (AWS)
**DuckDB:** 1.5.0 | **Extensions:** iceberg, uc_catalog, delta, httpfs

---

## Results

> **Note:** Foreign Iceberg tables are excluded from all tests — complex setup, low priority for this experiment. 🚫

### UC REST Connection (`uc_catalog` extension, Delta protocol)

| Table Type | Create Table | Write (Update) | Read | Write (Append) | Delete Row | Drop Table | Predicate Pushdown |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Managed Delta** | ❌¹ | ❌¹ | ❌¹ | ❌¹ | ❌¹ | ❌¹ | ❌¹ |
| **External Delta** | ❌¹ | ❌¹ | ❌¹ | ❌¹ | ❌¹ | ❌¹ | ❌¹ |
| **Managed Iceberg** | ❌¹ | ❌¹ | ❌¹ | ❌¹ | ❌¹ | ❌¹ | ❌¹ |
| **Foreign Iceberg** | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 |

> ¹ `uc_catalog` extension has a hostname resolution bug — endpoint URL is not prepended to API paths, causing all requests to fail. Tested with DuckDB 1.5.0.

### Iceberg REST Connection (`iceberg` extension)

| Table Type | Create Table | Write (Update) | Read | Write (Append) | Delete Row | Drop Table | Predicate Pushdown |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Managed Delta** | ❌² | ❌² | ❌² | ❌² | ❌² | ❌² | ❌² |
| **External Delta** | ❌² | ❌² | ❌² | ❌² | ❌² | ❌² | ❌² |
| **Managed Iceberg** | ❌⁴ | ❌³ | ❌³ | ❌³ | ❌³ | ❌³ | ❌³ |
| **Foreign Iceberg** | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 |

> ² Delta tables without UniForm are rejected: "Table is not an Iceberg compatible table"
> ³ Credential vending returns empty `storage-credentials` — DuckDB gets S3 403 when accessing data files
> ⁴ CREATE TABLE returns HTTP 403 Forbidden: "Not authorized to make this request"

### Delta Sharing Protocol (no native DuckDB client)

| Table Type | Create Table | Write (Update) | Read | Write (Append) | Delete Row | Drop Table | Predicate Pushdown |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Managed Delta** | ❌⁵ | ❌⁵ | ❌⁶ | ❌⁵ | ❌⁵ | ❌⁵ | ❌⁶ |
| **External Delta** | ❌⁵ | ❌⁵ | ❌⁶ | ❌⁵ | ❌⁵ | ❌⁵ | ❌⁶ |
| **Managed Iceberg** | ❌⁵ | ❌⁵ | ❌⁶ | ❌⁵ | ❌⁵ | ❌⁵ | ❌⁶ |
| **Foreign Iceberg** | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 |

> ⁵ Delta Sharing protocol is read-only by design — no write endpoints exist in the spec
> ⁶ DuckDB has no native Delta Sharing client. Reads require a Python bridge (delta-sharing lib → pandas → DuckDB), which is out of scope for pure DuckDB testing

---

**Legend:** ➖ not yet tested &nbsp;|&nbsp; ✅ works &nbsp;|&nbsp; ❌ fails &nbsp;|&nbsp; ⚠️ partial &nbsp;|&nbsp; 🚫 out of scope

## Key Findings

### What works
- **Iceberg REST catalog metadata** — DuckDB successfully attaches to UC, lists schemas, and discovers all tables (Delta and Iceberg) via the Iceberg REST endpoint
- **OAuth token auth** — Databricks SDK OAuth tokens work for catalog operations (no PAT required)

### What doesn't work (and why)

1. **Credential vending is broken for managed storage** — UC's Iceberg REST endpoint returns an empty `storage-credentials` list when asked for vended credentials (`X-Iceberg-Access-Delegation: vended-credentials`). Without S3 credentials, DuckDB cannot access data files → HTTP 403. This may require a workspace-level feature flag or specific storage credential configuration.

2. **`uc_catalog` extension has a URL construction bug** — The extension fails to prepend the configured endpoint hostname to API paths, causing all HTTP requests to hit invalid URLs. Affects DuckDB 1.5.0.

3. **Delta tables are invisible via Iceberg REST** — Only tables with UniForm (Iceberg compatibility) enabled are visible through the Iceberg REST endpoint. Pure Delta tables are rejected with "not an Iceberg compatible table."

4. **Delta Sharing has no DuckDB client** — The `delta` extension reads storage directly (bypasses UC). There is no native Delta Sharing REST protocol client in DuckDB.

5. **Delta Sharing is read-only by protocol design** — Even with a Python bridge, only reads are possible. No write endpoints exist in the spec.

### Recommendations

- **Wait for credential vending fix** — The Iceberg REST path is architecturally correct and should provide full CRUD once credential vending works. This may require Databricks workspace configuration changes or a DuckDB extension update.
- **For reads today** — Use the Python bridge pattern (delta-sharing → pandas → DuckDB) as a workaround, accepting no pushdown.
- **For writes today** — Use Databricks Connect / DBSQL for writes, DuckDB for local analytics on exported data.

## Scripts

| Script | Description |
|--------|-------------|
| `scripts/00_setup_tables.py` | Creates test tables (managed_delta, external_delta, managed_iceberg) via Databricks Connect |
| `scripts/01_iceberg_rest.py` | Tests all operations via Iceberg REST catalog endpoint |
| `scripts/02_delta_uc_rest.py` | Tests Delta Sharing REST + delta_scan direct storage paths |

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (Python package manager)
- Databricks CLI configured (`~/.databrickscfg` with `databricks-cli` auth type)
- DuckDB >= 1.4.0 (installed via `uv sync`)

## Running

```bash
uv sync
uv run python scripts/00_setup_tables.py    # Create test tables
uv run python scripts/01_iceberg_rest.py     # Test Iceberg REST path
uv run python scripts/02_delta_uc_rest.py    # Test Delta / UC REST path
```
