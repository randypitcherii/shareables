# Iceberg v3 deletion-vector files: Databricks vs DuckDB/PyIceberg (2026-11-02)

## Observation

Databricks registers its deletion vectors in Iceberg metadata as Puffin files, but the files it writes don't have Puffin framing. DuckDB 2.0.0 and PyIceberg 0.12.0 check that framing by default, so they refuse to read any table that has one of these files. That includes a managed Delta table read through Iceberg reads, because the same files are listed in its Iceberg metadata.

## Evidence

`iceberg_metadata()` from DuckDB on `dml_iceberg` (paths redacted) after this sequence:
1. Databricks ran `UPDATE` and then `DELETE` (setup).
2. DuckDB ran `UPDATE`, `DELETE` and `MERGE` with `iceberg_unsafe_skip_puffin_verification` (row 01).

| Writer | `content` | `file_format` in metadata | File name | Size | First bytes | Last bytes |
|---|---|---|---|---|---|---|
| Databricks | POSITION_DELETES | `PUFFIN` | `deletion_vector_<uuid>.bin` | 45 B | `01 00 00 00 24 d1 d3 39` | `… e2 be 92 4e` |
| Databricks | POSITION_DELETES | `PUFFIN` | `deletion_vector_<uuid>.bin` | 43 B | `01 00 00 00 22 d1 d3 39` | `… f7 a6 b4 b5` |
| DuckDB | POSITION_DELETES | `puffin` | `<uuid>-deletes.puffin` | 490 B | `PFA1 …` | `… PFA1` |

The Puffin spec requires a file to start with the magic `PFA1` and end with a footer that also ends in `PFA1`. The Databricks files start with a version byte instead and have no footer. The errors are:

- **DuckDB 2.0.0:** `Puffin file '…' has invalid trailing magic. Older versions of DuckDB wrote deletion vector files as bare blobs. To read those files, run "SET iceberg_unsafe_skip_puffin_verification = true"`
- **PyIceberg 0.12.0:** `Incorrect magic bytes, expected b'PFA1', got b'\x01\x00\x00\x00'`

## Scope of the claim

- These are the measured bytes and client errors. I didn't test whether Java Iceberg / Spark (which read DV blobs by offset and length) or other engines accept the files.
- With the skip-verification setting, DuckDB reads these tables and its UPDATE/DELETE/MERGE produce exactly the rows Databricks reads back (row 01). So the blob contents work; only the file framing fails strict checks.
- Tables where Databricks never ran a row-level change (append only) have no such files, and strict DuckDB DML works on them (row 01 control).
- Turning off deletion vectors (`'iceberg.enableDeletionVectors' = 'false'`) on a Liquid v3 table makes Databricks UPDATE/DELETE rewrite files instead, and strict DuckDB and PyIceberg reads work again (row 06). The cost: DuckDB UPDATE on that table then fails with HTTP 500 at commit, because DuckDB only writes merge-on-read deletes.
