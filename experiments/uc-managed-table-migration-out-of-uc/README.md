# Can you migrate out of Unity Catalog managed tables?

This experiment tests practical exit paths from UC managed Delta and managed Iceberg tables. It separates live behavior from product claims: every verdict below must come from a numbered script and a scrubbed entry in `results/matrix_results.json`.

## Findings matrix

Verified run: **2026-09-02**. One real AWS workspace, a serverless SQL warehouse, an isolated UC schema, a dedicated S3 prefix, AWS Glue, Amazon Athena, and synthetic three-row source tables.

| # | Capability / question | Result | Evidence |
|---|---|---|---|
| 1 | Copy managed Delta to an uncataloged Delta path | ✅ | A path-based Delta CTAS copied all three rows. See `scripts/migrations/01_delta_to_uncataloged_path.py` and result row 1. |
| 2 | Copy managed Delta to a UC external Delta table | ✅ | CTAS copied all rows to a table whose location was verified under customer-owned storage. See script/result row 2. |
| 3 | Unregister and re-register managed Delta without a data copy | ❌ | `UNREGISTER TABLE` returned `PARSE_SYNTAX_ERROR` on DBSQL 2026.20; the claimed no-copy sequence could not start. See script/result row 3. |
| 4 | Migrate managed Iceberg to a non-Databricks Iceberg catalog | ✅ | A Parquet staging copy moved all rows to AWS Glue Iceberg; Athena updated and reread the destination. See script/result row 4. |
| 5 | Copy managed Delta and register it in AWS Glue for Athena | ✅ | Athena rejected the default Delta protocol, then registered and read a compatibility-targeted copy with all three rows. See script/result row 5. |

Status vocabulary: ✅ works as tested · ❌ does not work as tested · ◑ partial · ❓ not isolated.

## Leaving works, but it is a migration—not an unregister operation

- **Delta can leave UC through a data copy.** Both uncataloged Delta and UC external Delta destinations preserved all three test rows.
- **Default Delta output was not automatically portable to Athena.** Athena reported `Delta protocol version is too new for Athena DDL engine`.
- **A compatibility-targeted Delta copy worked in AWS Glue/Athena.** It used reader version 1, writer version 2, classic checkpoints, and deletion vectors disabled.
- **Managed Iceberg moved into AWS Glue Iceberg through a Parquet staging copy.** Athena verified all rows and wrote an update in the destination.
- **Neither independent-catalog path preserved source table metadata.** Both copied data, and the destination catalog created new metadata.
- **The tested no-copy path does not exist.** `UNREGISTER TABLE` was unavailable on the tested DBSQL surface.

## Run it

Copy `.env.example` to `.env`. Set a writable dedicated external-location subdirectory, an authenticated AWS SSO profile, a temporary Glue database, and an Athena result prefix. Then run:

```bash
make sync
make verify
make matrix
make check
make cleanup
```

`make matrix` creates isolated source tables with three synthetic rows. It never uses customer data. `make cleanup` removes the UC schema and temporary Glue database; path-based output files remain because deleting object-storage data requires the storage owner's explicit action.

## Migration guidance to evaluate after the capability matrix

1. **Profile interactions.** Inventory loading, transformation (including maintenance), and consumption separately.
2. **Parallel validation.** Reproduce loading and transformations in the destination, read-only against the source, until automated checks pass.
3. **Hard cutover.** Move writes and transformations, leave read-only pointers where supported, and continue validation.
4. **Migrate consumers in cohorts.** Prioritize strategic and expensive workloads, then scale old consumption infrastructure down as adoption moves through the long tail.

This guidance is a process hypothesis, not a measured matrix result.

## Safety

- OAuth/SSO profiles only; scripts refuse `dapi` personal access tokens.
- Real workspace URLs, identities, catalog names, and storage paths are scrubbed from committed results.
- Use a dedicated storage prefix. The experiment does not delete uncataloged object-storage files.
