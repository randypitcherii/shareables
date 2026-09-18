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

> **Status: run 2026-09-18.** Every cell below is backed by evidence in
> `results/matrix_results.json`. Tracking issue:
> [randypitcherii/shareables#87](https://github.com/randypitcherii/shareables/issues/87).
>
> **TL;DR — the hypothesis is confirmed and the bound is exact.** UC Glue
> federation fails on an Iceberg table iff the Glue `StorageDescriptor` type
> string contains the token `list<` *anywhere* (top-level or nested inside a
> `struct`). `map<>` and nested `struct<>` are fine. The AWS Glue Iceberg REST
> endpoint writes `list<>`; the native `GlueCatalog` writers write `array<>`.
> Athena reads the same table without complaint. Patching the SD by hand works
> for exactly zero writer commits. There is no UC-side lever.

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
the two catalog implementations above. Date of run: **2026-09-18**, DBSQL `2026.36` (serverless), pyiceberg 0.9.x writers.

| # | Capability / Question | Claim / Source | Result | Notes |
|---|---|---|---|---|
| 1 | Primitives-only Iceberg table via Glue REST endpoint → readable in UC | [Glue federation reads Iceberg (Public Preview)](https://docs.databricks.com/aws/en/query-federation/hms-federation-concepts#support) | ✅ | `DESCRIBE` + `SELECT COUNT(*)` = 3 rows. Federation itself works; everything below is a type finding |
| 2 | Same writer, `list<bigint>` column → `INTERNAL_ERROR_INVALID_HIVE_COLUMN_TYPE` | Field report | ❌ **reproduced** | SD type string is literally `list<bigint>`; `DESCRIBE` and `SELECT` both fail with the reported class |
| 3 | Same shape via native `GlueCatalog` → SD is `array<>` and UC reads it | `IcebergToGlueConverter` source | ✅ | SD type string is `array<bigint>`; identical Iceberg schema and data, UC reads 3 rows. **The writer path is the whole variable** |
| 4 | Which shapes trip it: `list<prim>`, `list<struct>`, `map<>`, `struct<list<>>`, nested-required struct — per writer | [Iceberg limitations](https://docs.databricks.com/aws/en/iceberg/#limitations) list only `UUID`, `Fixed`, `TIME`, nested-required `STRUCT` | ◑ | 6 shapes × 2 writers: **12/12 `GlueCatalog` cells ✅; REST cells fail iff the SD string contains `list<`** — `list<bigint>`, `list<struct<…>>`, and `struct<tags:list<string>,…>` all ❌; `map<string,string>`, `struct<struct<>>`, primitives ✅. The nested-required struct read fine on both |
| 5 | Failure identical across `DESCRIBE` / `SELECT` / `REFRESH FOREIGN TABLE` / `information_schema` / `SHOW CREATE`? | Field report: "all fail at resolution time" | ✅ confirmed | 7/7 table-level surfaces return the same `INTERNAL_ERROR_INVALID_HIVE_COLUMN_TYPE`, incl. `REFRESH FOREIGN TABLE` and `SHOW CREATE TABLE`. (`information_schema.columns` failed earlier with a Lake Formation `Describe on information_schema` denial — an environment artefact, not a type result) |
| 6 | `glue:UpdateTable` patch `list<>`→`array<>`: UC reads? Survives the next REST-endpoint commit? | Suggested workaround; reported to revert in ~60 s | ◑ | Patch → UC reads immediately ✅. **One** subsequent REST-endpoint append → SD is `list<bigint>` again and UC fails again ❌. `workaround_works_once=true`, `workaround_survives_commit=false` |
| 7 | Athena reads the same `list<>` SD table? | Athena reads Iceberg from `metadata.json` | ✅ | `COUNT(*)`=19, sample rows return arrays, and Athena's `DESCRIBE` reports `array<bigint>` — it never looks at the SD. **UC-federation-specific** |
| 8 | Any UC-side lever that sidesteps SD parsing (`REFRESH`, view, `CREATE TABLE USING iceberg LOCATION <metadata.json>`, `read_files`) | — | ❌ | `REFRESH` and `CREATE VIEW` hit the same error; `CREATE TABLE … USING iceberg LOCATION` is rejected (`MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED`); `read_files` on the parquet ✅ proves storage + credentials are fine — the block is purely metadata-side |

Status vocabulary: ✅ works as claimed · ❌ does not (with evidence) · ◑ partially · ❓ not yet isolated.

## Key findings

1. **The bound is a single token.** UC Glue federation constructs the Spark
   `CatalogTable` from the Glue SD Hive type strings and rejects any column
   whose type string contains `list<` — including when the `list` is buried in
   a `struct`. `array<`, `map<`, and nested `struct<` parse fine. Row 4 is the
   evidence: same six Iceberg schemas, the only cells that fail are the ones
   whose SD string contains `list<`.
2. **The writer path, not the schema, decides readability.** Identical Iceberg
   schema + identical data: pyiceberg `GlueCatalog` → SD `array<bigint>` → UC
   ✅; the AWS Glue Iceberg REST endpoint → SD `list<bigint>` → UC ❌. A
   streaming engine writing through the REST endpoint cannot influence the SD
   it gets.
3. **It is UC-specific.** Athena reads the very same `list<>`-SD table and
   reports the column as `array<bigint>` because it resolves schema from
   `metadata.json`. `read_files` on the underlying parquet also works from UC,
   so storage, credentials, and the Iceberg data itself are all fine — the
   failure is entirely in the HMS→Spark metadata conversion.
4. **The hand-patch workaround is not a workaround under a live writer.**
   Patching the SD to `array<>` makes UC read the table *once*; the very next
   REST-endpoint commit rewrites the SD back to `list<>` and UC fails again.
   With a streaming writer committing every few seconds the window is
   effectively zero.
5. **There is no UC-side lever.** `REFRESH FOREIGN TABLE`, a view over the
   foreign table, and `CREATE TABLE … USING iceberg LOCATION <metadata.json>`
   all fail. The fix has to land in either the federation layer (derive schema
   from `metadata.json`, or normalise `list`→`array` in the converter) or the
   Glue REST endpoint's SD mapping — or the workload has to switch to a writer
   that produces Hive-canonical SD strings.

### For the customer conversation

- **Short term:** if the writer can be pointed at Glue via a native
  `GlueCatalog` implementation (Iceberg Java/pyiceberg/Spark `GlueCatalog`,
  Flink Glue catalog) instead of the REST endpoint, UC reads the tables today.
  Row 3 demonstrates this with no other change.
- **If the REST endpoint is non-negotiable:** the tables are unreadable via
  Glue federation until a platform fix ships. Athena and any Iceberg-native
  reader are unaffected.
- **Detection:** row 2's `sd_type_of_list_column` field flips the moment either
  AWS or Databricks changes behaviour — re-run `make run-02` to check.

### Setup gotchas worth keeping (all encoded in `terraform/` now)

- **Self-assuming IAM role cannot name its own ARN at creation.** IAM rejects
  the trust policy with `Invalid principal in policy`. Trust the account root
  with an `aws:PrincipalArn` condition on the role's ARN instead — same
  semantics, valid on first apply.
- **UC does not use the account id as `sts:ExternalId`.** Both the service
  credential and the storage credential were minted with a fresh per-credential
  UUID. The trust policy has to accept those (`uc_credential_external_ids`),
  which forces a two-pass `tf-apply → setup-uc → tf-apply → setup-uc` flow.
  Expect ~3 minutes of IAM propagation before `storage-credentials validate`
  goes green.
- **Lake Formation enforcement.** This sandbox has the IAM-only defaults
  cleared, so the federation role needs explicit LF `DESCRIBE` on `default`
  (UC's Hive client probes it on connect) and on the experiment database, plus
  `SELECT`/`DESCRIBE` on its tables. Without the `default` grant `SHOW SCHEMAS`
  fails with `Insufficient Lake Formation permission(s): Required Describe on default`.
- **Athena `DESCRIBE` wants backticks**, not double-quoted identifiers.

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
make setup-uc                                 # creates the UC credentials, then fails on the external location (expected)
#   paste both credentials' aws_iam_role.external_id into terraform.tfvars -> uc_credential_external_ids
make tf-apply setup-uc                        # second pass: trust policy now accepts UC's external ids
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
