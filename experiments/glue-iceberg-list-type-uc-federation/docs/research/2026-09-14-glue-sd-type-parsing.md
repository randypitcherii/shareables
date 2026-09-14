# Why a Glue Iceberg table with a `list` column is unreadable through UC federation

_2026-09-14. Background for the experiment; public-safe summary of a field
investigation plus source reading. Nothing here is a measured result — see
`results/matrix_results.json` for those._

## The symptom

A Glue-registered Iceberg table, written continuously by a streaming engine
through an Iceberg REST catalog, fails in Unity Catalog on every SQL surface —
`DESCRIBE`, `DESCRIBE EXTENDED`, `DESCRIBE DETAIL`, `SELECT` — with

```
[INTERNAL_ERROR_INVALID_HIVE_COLUMN_TYPE] Failed to convert Hive table to Spark catalog table.
```

Other Iceberg tables in the same foreign catalog read correctly. The only
structural difference: the failing table has a column of Iceberg type
`list<long>`.

The Glue registration itself is correct — `table_type = ICEBERG`, a valid
`metadata_location`, and an Iceberg `metadata.json` whose schema says
`list_ids: list<long>`. What is *off* is the Hive-compatible mirror of that
schema in the Glue `StorageDescriptor.Columns`: the column's `Type` string is
`list<bigint>`, Iceberg's token, where Hive's grammar expects `array<bigint>`.

## The mechanism (from code-path analysis, not yet reproduced here)

Glue federation in Databricks treats Glue as a Hive metastore. On table
resolution it:

1. Fetches the Glue table and builds a Spark `CatalogTable` from the
   StorageDescriptor's Hive type strings —
   `HiveClientImpl.convertHiveTableToCatalogTable` → `getSparkSQLDataType` →
   `CatalystSqlParser.parseDataType("list<bigint>")`.
2. `list` is not a valid Hive `TypeInfo` token, so parsing throws, and the
   exception is rewrapped as `INTERNAL_ERROR_INVALID_HIVE_COLUMN_TYPE`.
3. The path that *would* read the schema from Iceberg `metadata.json`
   (the UniForm foreign-Iceberg transform) runs **after** `CatalogTable`
   construction, so it is never reached.

Consequences that follow directly:

- The failure is at resolution, so every surface fails identically (row 5
  tests this).
- No Spark/DBSQL flag can bypass it — `enableOnFederation.hms`-style toggles
  are evaluated after the crash point.
- The documented Iceberg type limitations (`UUID`, `Fixed(L)`, `TIME`, nested
  `STRUCT` with required fields) are *not* what is being hit; `list` is a fully
  supported Iceberg → Spark `ARRAY` mapping. The problem is one layer below,
  in Hive-type-string parsing.

Engineering's stated position, paraphrased: Glue federation fundamentally
expects every column to be a valid Hive column; the correct type is inferable
from Iceberg metadata, but changing federation to consult it is a design
decision with wider implications, and no fix is committed in any current DBR.

## Why the writer path is the variable

The Hive type string in the SD is written by whatever Iceberg catalog
implementation commits to Glue. They do not agree:

| Implementation | LIST → SD `Type` |
|---|---|
| `org.apache.iceberg.aws.glue.GlueCatalog` (Java) | `array<…>` — see [`IcebergToGlueConverter.java`, `case LIST`](https://github.com/apache/iceberg/blob/main/aws/src/main/java/org/apache/iceberg/aws/glue/IcebergToGlueConverter.java): `String.format("array<%s>", …)` |
| pyiceberg `GlueCatalog` | `array<…>` — mirrors the Java converter |
| AWS Glue **Iceberg REST endpoint** (`glue.<region>.amazonaws.com/iceberg`) | `list<…>` (reported) — the mapping is performed by the AWS service, not the client |

The reported failing configuration is `org.apache.iceberg.rest.RESTCatalog`
(Iceberg 1.11) pointed at the Glue REST endpoint. In that arrangement the
streaming engine never touches the Glue SD; the REST service does. So "have
the writer emit `array<>`" is not a knob the table owner has.

This is why rows 2 and 3 of the matrix write the **same schema** through the
two implementations into the **same Glue database** and compare (a) the SD
type string and (b) UC's verdict. If row 3 reads and row 2 does not, the
defect is bounded to the REST-endpoint writer path.

## Why the obvious workaround is not one

`aws glue update-table` rewriting `list<bigint>` → `array<bigint>` in the SD,
with `metadata_location` and `table_type` untouched, does make UC resolve the
table. But a streaming writer commits every few seconds to a minute, and each
commit through the REST endpoint rewrites the SD — the patch is reported to
survive roughly one commit interval. Row 6 measures exactly this: patch, read,
commit once, read again.

## Adjacent failure modes on the same surface (not this bug)

Worth knowing when reading results, because they produce different error
classes on nearby tables:

- **`browse_only` / `UNSUPPORTED_FEATURE.TABLE_OPERATION … does not support
  batch scan`** — seen when Glue's `StorageDescriptor.Location` is empty
  (some Athena-written Iceberg tables). `storage_root` on the foreign
  catalog is required for Iceberg reads and inheriting from the metastore is
  not sufficient; see the [Glue federation troubleshooting section](https://docs.databricks.com/aws/en/query-federation/hms-federation-glue#iceberg-table-reads-fail).
- **Column-mapping ID collisions** in the Iceberg → Delta metadata conversion
  — a different stage (after resolution) and a different error.
- **`DELTA_COMMAND_INVARIANT_VIOLATION` on `REFRESH FOREIGN TABLE` /
  `DESCRIBE DETAIL`** — conversion-stage, not resolution-stage.

Row 4's sweep records the error class per cell precisely so these can be told
apart from the `list` parse failure.

## What would make this moot

- Federation deriving the foreign Iceberg schema from `metadata.json` when
  `table_type = ICEBERG`, or normalizing `list`→`array` in the HMS→Spark type
  converter. Either is a Databricks-side change.
- Unity Catalog federating **to an Iceberg REST catalog** directly (Glue's
  REST endpoint being one) instead of to Glue-as-HMS. The Hive SD would not be
  in the path at all. This is on the public "catalog of catalogs" narrative
  but is not a documented federation target at time of writing.
- AWS changing the Glue REST endpoint's SD mapping to Hive-canonical tokens.

## Public references

- [Hive metastore federation — requirements and feature support](https://docs.databricks.com/aws/en/query-federation/hms-federation-concepts#support)
- [Enable Hive metastore federation for AWS Glue](https://docs.databricks.com/aws/en/query-federation/hms-federation-glue)
- [What is Apache Iceberg in Databricks? — Limitations](https://docs.databricks.com/aws/en/iceberg/#limitations)
- [Connecting to the Data Catalog using AWS Glue Iceberg REST endpoint](https://docs.aws.amazon.com/glue/latest/dg/connect-glu-iceberg-rest.html)
- [`IcebergToGlueConverter.java`](https://github.com/apache/iceberg/blob/main/aws/src/main/java/org/apache/iceberg/aws/glue/IcebergToGlueConverter.java)
