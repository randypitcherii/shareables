# DuckDB + Unity Catalog Experiment

**Date:** 2026-03-11

This experiment explores how DuckDB can interact with Databricks Unity Catalog using its built-in catalog integrations. It tests read/write capabilities across UC connection types and table formats to understand what's actually supported versus what fails silently or throws errors.

---

## Results

> **Note:** Foreign Iceberg tables are excluded from all tests — complex setup, low priority for this experiment. 🚫

### UC REST Connection

| Table Type | Create Table | Write (Update) | Read | Write (Append) | Delete Row | Drop Table | Predicate Pushdown |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Managed Delta** | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ |
| **External Delta** | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ |
| **Managed Iceberg** | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ |
| **Foreign Iceberg** | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 |

### Iceberg REST Connection

| Table Type | Create Table | Write (Update) | Read | Write (Append) | Delete Row | Drop Table | Predicate Pushdown |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Managed Delta** | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ |
| **External Delta** | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ |
| **Managed Iceberg** | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ |
| **Foreign Iceberg** | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 | 🚫 |

---

**Legend:** ➖ not yet tested &nbsp;|&nbsp; ✅ works &nbsp;|&nbsp; ❌ fails &nbsp;|&nbsp; ⚠️ partial &nbsp;|&nbsp; 🚫 out of scope
