# Can you migrate out of Unity Catalog managed tables?

This experiment tests practical exit paths from UC managed Delta and managed Iceberg tables. It separates live behavior from product claims: every verdict below must come from a numbered script and a scrubbed entry in `results/matrix_results.json`.

## Findings matrix

First run: **2026-09-02**. One real AWS workspace, a serverless SQL warehouse, an isolated UC schema, and synthetic three-row source tables. The configured external location was visible to UC but its AWS role was unhealthy.

| # | Capability / question | Result | Evidence |
|---|---|---|---|
| 1 | Copy managed Delta to an uncataloged Delta path | ❓ | The request reached storage but credential vending failed because the external location's AWS role was misconfigured. See `scripts/migrations/01_delta_to_uncataloged_path.py` and result row 1. |
| 2 | Copy managed Delta to a UC external Delta table | ❓ | Cloud storage returned 403 for the configured external location. The migration capability was not isolated. See script/result row 2. |
| 3 | Unregister and re-register managed Delta without a data copy | ❌ | `UNREGISTER TABLE` returned `PARSE_SYNTAX_ERROR` on DBSQL 2026.20; the claimed no-copy sequence could not start. See script/result row 3. |
| 4 | Migrate managed Iceberg to a non-Databricks Iceberg catalog | ❓ | The managed Iceberg source was created and read with three rows, but no independent destination catalog was configured. See script/result row 4. |

Status vocabulary: ✅ works as tested · ❌ does not work as tested · ◑ partial · ❓ not isolated.

## The first run disproves one path and exposes two setup gaps

- **The no-copy Delta path is not available through `UNREGISTER TABLE` on the tested SQL surface.** This is a live parser result, not an inference about storage.
- **Managed Iceberg creation and reads work.** That proves only the starting point; it does not prove migration to another catalog.
- **Rows 1–2 need a healthy dedicated external location.** A visible UC object is not proof that its cloud role can vend working credentials.
- **Row 4 needs an independent catalog.** The harness records `inconclusive` rather than treating source-table readability as portability.

## Run it

Copy `.env.example` to `.env`, set a writable dedicated external-location subdirectory, then run:

```bash
make sync
make verify
make matrix
make check
make cleanup
```

`make matrix` creates isolated source tables with three synthetic rows. It never uses customer data. `make cleanup` removes the UC schema; path-based output files remain because deleting object-storage data requires the storage owner's explicit action.

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
