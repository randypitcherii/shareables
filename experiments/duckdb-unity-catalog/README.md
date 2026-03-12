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

### Iceberg REST (`iceberg` extension, Iceberg reader/writer)

| Table Type | Read | Predicate Pushdown | Write (Append) | Write (Update) | Delete Row | Create Table | Drop Table |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Managed Delta** (UniForm) | ✅ | ✅ | ❌ | ❌ | ❌ | -- | -- |
| **Managed Iceberg** | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ |

### UC REST (`uc_catalog` extension, Delta reader/writer)

| Table Type | Read | Predicate Pushdown | Write (Append) | Write (Update) | Delete Row |
|---|:---:|:---:|:---:|:---:|:---:|
| **Managed Delta** (UniForm) | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Managed Iceberg** | ✅ | ✅ | ❌ | ❌ | ❌ |

**Legend:** ✅ works | ❌ fails

**Bottom line:** DuckDB + Unity Catalog is read-only in practice today. No write path works.
