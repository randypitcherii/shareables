# DuckDB + Unity Catalog Experiment

This experiment tests DuckDB's read/write capabilities against Databricks Unity Catalog across two connection paths and two table formats, to understand what actually works today.

## Expectations

DuckDB offers two connection paths to Unity Catalog. Each path uses a different reader/writer and is designed for a specific table format:

| Path | Extension | Reader/Writer | Reads | Writes |
|---|---|---|---|---|
| **Iceberg REST** | `iceberg` | Iceberg | All tables (Delta+UniForm, Iceberg) | Managed Iceberg only |
| **UC REST** | `uc_catalog` + `delta` | Delta | All tables (Delta, Iceberg) | Managed Delta only |

- A Delta reader should never be used with an Iceberg REST catalog (and vice versa)
- Writes should only target the native format for each path

## Results

**Verified** 2026-03-12 via `uv run pytest tests/ -v` — 26 tests (6 passed, 20 xfailed)

### Legend

- ✅ Actual success, success expected
- 🤔 Actual success, failure expected (unexpected)
- ☑️ Actual failure, failure expected (expected limitation)
- ❌ Actual failure, success expected (bug)

### Iceberg REST (`iceberg` extension, Iceberg reader/writer)

| Table Type | Read | Predicate Pushdown | Write (Append) | Write (Update) | Delete Row | Create Table | Drop Table |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Managed Delta** (UniForm) | ✅ | ✅ | ☑️ | ☑️ | ☑️ | -- | -- |
| **Managed Iceberg** | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ |

- **Managed Iceberg reads/writes fail** due to credential scope bug — [duckdb/duckdb-iceberg#792](https://github.com/duckdb/duckdb-iceberg/issues/792)
- Predicate pushdown works (metadata-only, not affected by credential bug)

### UC REST (`uc_catalog` extension, Delta reader/writer)

| Table Type | Read | Predicate Pushdown | Write (Append) | Write (Update) | Delete Row |
|---|:---:|:---:|:---:|:---:|:---:|
| **Managed Delta** (UniForm) | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Managed Iceberg** | ✅ | ✅ | ☑️ | ☑️ | ☑️ |

- **All Managed Delta operations fail** with Bad Request from temporary-table-credentials API — [duckdb/uc_catalog#68](https://github.com/duckdb/uc_catalog/issues/68), [duckdb/duckdb-delta#289](https://github.com/duckdb/duckdb-delta/issues/289)

## Bottom Line

DuckDB + Unity Catalog is read-only in practice today. Both paths have bugs blocking their primary write targets: Iceberg REST can't write managed Iceberg tables, and UC REST can't access managed Delta tables at all.
