# Bounds of Unity Catalog Glue federation for Iceberg tables with `list` columns

A live evaluation of **which Glue-registered Iceberg tables Unity Catalog can
actually read through Glue (HMS) federation**, when the table has complex
columns — `list`, `map`, `struct` — and *how the table was written* varies.

The trigger is a field-reported failure: a Glue Iceberg table written by a
streaming engine through an Iceberg REST catalog is completely unreadable in
UC, while sibling tables without list columns read fine. Every SQL surface
fails at table-resolution time with:

```
[INTERNAL_ERROR_INVALID_HIVE_COLUMN_TYPE] Failed to convert Hive table to Spark catalog table.
```

The goal is a clarity grid, not a workaround hunt. **A "not readable" cell with
a verbatim error class is a first-class result.**

> **Status: scaffolded, not yet run.** The matrix below is the plan; every cell
> is ❓ until a script has written its evidence to `results/matrix_results.json`.
> Tracking issue: [randypitcherii/shareables#87](https://github.com/randypitcherii/shareables/issues/87).

---

## The hypothesis under test

Glue federation constructs the Spark `CatalogTable` from the **Glue
`StorageDescriptor` Hive type strings** *before* the Iceberg
schema-from-`metadata.json` path runs. If the SD carries Iceberg's native
`list<bigint>` token instead of Hive's canonical `array<bigint>`, the Hive type
parser rejects `list` and the Iceberg path never gets a turn.

Which token lands in the SD depends on **who wrote the Glue table**:

| Writer path | SD type for an Iceberg `list<long>` | Source |
|---|---|---|
| Iceberg Java `GlueCatalog` | `array<bigint>` | [`IcebergToGlueConverter.java` — `case LIST` → `array<%s>`](https://github.com/apache/iceberg/blob/main/aws/src/main/java/org/apache/iceberg/aws/glue/IcebergToGlueConverter.java) |
| pyiceberg `GlueCatalog` | `array<bigint>` | mirrors the Java converter |
| **AWS Glue Iceberg REST endpoint** (`https://glue.<region>.amazonaws.com/iceberg`) | `list<bigint>` *(reported)* | server-side mapping; the client has no say |

A streaming writer that talks to Glue *through the REST endpoint* — the
reported configuration — therefore cannot fix this from its side. Whether that
is the whole story is exactly what rows 2 and 3 measure.

## Findings matrix

Environment shape: one AWS account/region, one Glue database, one Databricks
workspace on AWS with a serverless SQL warehouse, foreign catalog created with
`storage_root` set (required for Iceberg reads). Writers are pyiceberg over
the two catalog implementations above. Date of run: _not yet run_.

| # | Capability / Question | Claim / Source | Result | Notes |
|---|---|---|---|---|
| 1 | Primitives-only Iceberg table via Glue REST endpoint → readable in UC | [Glue federation reads Iceberg (Public Preview)](https://docs.databricks.com/aws/en/query-federation/hms-federation-concepts#support) | ❓ | Setup sanity — if this fails nothing else is a type finding |
| 2 | Same writer, `list<bigint>` column → `INTERNAL_ERROR_INVALID_HIVE_COLUMN_TYPE` | Field report | ❓ | Records SD type string **and** UC outcome separately |
| 3 | Same shape via native `GlueCatalog` → SD is `array<>` and UC reads it | `IcebergToGlueConverter` source | ❓ | Isolates the writer path as the variable |
| 4 | Which shapes trip it: `list<prim>`, `list<struct>`, `map<>`, `struct<list<>>`, nested-required struct — per writer | [Iceberg limitations](https://docs.databricks.com/aws/en/iceberg/#limitations) list only `UUID`, `Fixed`, `TIME`, nested-required `STRUCT` | ❓ | 6 shapes × 2 writers grid |
| 5 | Failure identical across `DESCRIBE` / `SELECT` / `REFRESH FOREIGN TABLE` / `information_schema` / `SHOW CREATE`? | Field report: "all fail at resolution time" | ❓ | One error class everywhere ⇒ CatalogTable construction |
| 6 | `glue:UpdateTable` patch `list<>`→`array<>`: UC reads? Survives the next REST-endpoint commit? | Suggested workaround; reported to revert in ~60 s | ❓ | Durability under a live writer is the load-bearing half |
| 7 | Athena reads the same `list<>` SD table? | Athena reads Iceberg from `metadata.json` | ❓ | Is the SD load-bearing for Iceberg readers generally, or only for UC federation? |
| 8 | Any UC-side lever that sidesteps SD parsing (`REFRESH`, view, `CREATE TABLE USING iceberg LOCATION <metadata.json>`, `read_files`) | — | ❓ | Expect ❌; `read_files` proves storage + creds are fine |

Status vocabulary: ✅ works as claimed · ❌ does not (with evidence) · ◑ partially · ❓ not yet isolated.

## Key findings

_To be written from `results/matrix_results.json` after the run._

## What would change the answer

- **A federation-layer fix** that derives the foreign Iceberg schema from
  `metadata.json` (or normalizes `list`→`array` in the HMS→Spark converter).
  Not present in any current DBR per the field report; re-run rows 2 and 5 on
  each new DBSQL/DBR channel to detect it.
- **Iceberg REST catalog federation** in Unity Catalog (federating *to* an
  Iceberg REST catalog rather than to Glue-as-HMS) would bypass the Hive SD
  entirely. It is on the public roadmap narrative but not a documented
  federation target at time of writing; when it ships, add a row.
- **The Glue REST endpoint changing its SD mapping** to Hive-canonical
  `array<>`. Row 2's `sd_type_of_list_column` field detects this directly.

## Layout

```
Makefile                 # THE command surface — `make` shows everything
scripts/
  _common.py             # config, clients, type-shape catalog, redaction, fail-closed results
  00_setup_uc.py         # UC credentials + external location + Glue connection + foreign catalog
  01_teardown_uc.py      # reverse of the above, plus drop all experiment Glue tables
  verify.py              # one real call per auth surface
  matrix/                # one numbered script per matrix row
results/matrix_results.json   # committed evidence (redacted at write time)
terraform/               # S3 + Glue database + self-assuming IAM role + Athena workgroup
tests/                   # no-infra tests (default) + `infrastructure`-marked live checks
docs/research/           # background write-up
```

## Running it

```bash
cp template.env dev.env                       # fill in Databricks profile + warehouse id
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
make tf-init tf-apply                         # paste the printed env snippet into dev.env
make setup-uc                                 # Databricks-side objects
make verify                                   # every auth surface answers
make run-core                                 # rows 1–3: the burning question
make run-all                                  # the full grid
make teardown-uc tf-destroy                   # leave nothing behind
```

Auth is SSO/OAuth profiles only for both clouds. Helpers refuse PAT-shaped
tokens. Everything written to `results/` is redacted (account id, bucket, role
ARN, workspace host, emails → placeholders) because this repo is public.

## Related

- [`experiments/starrocks-vs-dbsql-uc-formats`](../starrocks-vs-dbsql-uc-formats) — the
  read-performance side of the same customer conversation.
- [`docs/research/2026-09-14-glue-sd-type-parsing.md`](docs/research/2026-09-14-glue-sd-type-parsing.md) —
  background on the root cause and why the writer path is the variable.
