# Can you leave Unity Catalog managed tables without copying data?

**Answer: yes — if you leave through Iceberg.** Any Iceberg catalog can adopt the table's existing `metadata.json` in place (✅). Reading the Delta log directly off S3 also works, but only some engines accept UC's default Delta protocol (◑). The one thing that does *not* work is re-registering the same files as a UC *external* table (❌): UC reserves its managed-storage prefix.

Setup: a UC **managed Delta** table (UniForm on — Delta's option to also write Iceberg metadata beside the Delta log) and a UC **managed Iceberg** table, in a catalog whose storage root is your own S3 bucket. Three exits, each with **zero data copy**, using plain Python clients as the destination rather than a specific engine. Every verdict below comes from a numbered script and a scrubbed entry in `results/matrix_results.json`.

## Findings matrix

Verified run: **2026-09-03**. One real AWS workspace, a serverless SQL warehouse, a temporary UC catalog rooted in a customer-owned S3 prefix, synthetic three-row sources. Destinations are `duckdb` (delta extension), `deltalake` (delta-rs), and `pyiceberg` with a fresh SQLite-backed catalog.

| Exit path | From managed Delta (UniForm) | From managed Iceberg | Evidence |
|---|---|---|---|
| 1. Zero-catalog Delta on S3 (read the `_delta_log` in place)¹ | ◑ | ◑ | duckdb read 3/3 rows straight off the managed path. deltalake refused both tables' protocol features. `exit_01_zero_catalog_delta.py`, rows 1a/1b. |
| 2. Re-register as a UC **external** Delta table (same files) | ❌ | ❌ | `LOCATION_OVERLAP`: UC refuses `CREATE TABLE … LOCATION` on any path under `__unitystorage/` (the directory UC creates under the catalog's storage root for managed-table files), live or after `DROP TABLE`. Control: the same files copied outside that prefix registered and read 3/3. `exit_02_uc_external_reregister.py`, rows 2a/2b. |
| 3. Iceberg in a **new managing catalog** (adopt existing metadata.json) | ✅ | ✅ | A fresh pyiceberg catalog registered the newest metadata.json, read 3/3, then appended a row in place (4/4 readable; new snapshot committed under the source path). `exit_03_iceberg_new_catalog.py`, rows 3a/3b. |

¹ UC writes a Delta log for managed Iceberg tables too, so this exit applies to both columns.

Legend: ✅ every client succeeded, all rows · ◑ at least one client succeeded (the storage exit works; engine support varies) · ❌ no no-copy path worked · ❓ inconclusive (infrastructure failed before the capability was tested; none this run). Scripts run in order 1, 3, 2 because exit 2 drops the sources.

## What this means

- **The hypothesis held for the two catalog-free exits.** Both source formats carry a Delta log *and* Iceberg metadata, so the two starting points behaved identically in every row. Format is not the lock-in question; the catalog prefix is.
- **Iceberg is the clean no-copy exit.** Hand the newest `metadata.json` to any Iceberg catalog and it owns the table from there, including new commits into the same S3 prefix. UniForm writes it under `metadata/`, managed Iceberg under `_iceberg/metadata/`.
- **Delta-on-S3 works, but engine protocol support is uneven.** Default managed Delta uses reader 2/writer 7 with column mapping and IcebergCompatV2; managed Iceberg's Delta log adds v2 checkpoints and type widening. duckdb reads both; deltalake 1.6 reads neither. That is a client capability gap, not a storage or catalog block.
- **You cannot re-adopt the same bytes as a UC external table.** The `__unitystorage/` prefix is reserved even after the managed table (or the whole catalog) is dropped. Staying in UC as *external* requires a copy; leaving UC entirely does not.
- **No-copy cutover is a hard cutover (one-way, no dual-catalog period).** Once the new catalog commits a snapshot, UC still points at the old one and the two will never reconcile. Freeze UC writes first, or drop the UC table.
- **Sequence the UC drop deliberately.** Dropping a UC managed table schedules its files for deletion after a retention window (documented by UC; not measured here). Files were still present immediately after `DROP TABLE` in this run, but a real migration should have the new catalog owning the table and consumers moved before the UC side is dropped.

## Suggested migration sequence (untested process, not a matrix result)

1. **Profile interactions.** Inventory loading, transformation (including maintenance), and consumption separately.
2. **Parallel validation.** Reproduce loading and transformations in the destination, read-only against the source, until automated checks pass.
3. **Hard cutover.** Freeze source writes before advancing the destination catalog pointer. Move writes and transformations, leave only read-only source pointers, and continue validation.
4. **Migrate consumers in cohorts.** Prioritize strategic and expensive workloads, then scale old consumption infrastructure down as adoption moves through the long tail.

## Run it

Copy `.env.example` to `.env`. You need a serverless SQL warehouse, a writable UC external location in your own S3 bucket, and an AWS SSO profile that can read that bucket directly (the Python clients bypass UC). Then run:

```bash
make sync
make verify
make matrix
make check
make cleanup
```

`make matrix` creates a temporary UC catalog rooted in your bucket, writes two three-row synthetic tables, and runs all six cells. Exit 2 drops the source tables, so it runs last. `make check` runs the client helpers against local Delta and Iceberg tables with no cloud access. `make cleanup` drops the catalog; object-storage files remain because deleting them is the storage owner's call.

## Safety

- OAuth/SSO profiles only; scripts refuse `dapi` personal access tokens.
- Real workspace URLs, identities, catalog names, and storage paths are scrubbed from committed results.
- Use a dedicated storage prefix. The experiment does not delete uncataloged object-storage files.
