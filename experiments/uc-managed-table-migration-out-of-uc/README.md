# Can you leave Unity Catalog managed tables without copying data?

Start with a UC **managed Delta** table (UniForm on) and a UC **managed Iceberg** table in a catalog whose storage root is your own S3 bucket. Try three exits, each with **zero data copy**, using plain Python clients as the destination instead of a specific engine. Every verdict below comes from a numbered script and a scrubbed entry in `results/matrix_results.json`.

## Findings matrix

Verified run: **2026-09-03**. One real AWS workspace, a serverless SQL warehouse, a temporary UC catalog rooted in a customer-owned S3 prefix, synthetic three-row sources. Destinations are `duckdb` (delta extension), `deltalake` (delta-rs), and `pyiceberg` with a fresh SQLite-backed catalog.

| Exit path | From managed Delta (UniForm) | From managed Iceberg | Evidence |
|---|---|---|---|
| 1. Zero-catalog Delta on S3 (read the `_delta_log` in place) | ◑ | ◑ | duckdb read 3/3 rows straight off the managed path. delta-rs refused both tables' protocol features. `exit_01_zero_catalog_delta.py`, rows 1a/1b. |
| 2. Re-register as a UC **external** Delta table (same files) | ❌ | ❌ | `LOCATION_OVERLAP`: UC refuses `CREATE TABLE … LOCATION` on any path under `__unitystorage`, live or after `DROP TABLE`. Control: the same files copied outside that prefix registered and read 3/3. `exit_02_uc_external_reregister.py`, rows 2a/2b. |
| 3. Iceberg in a **new managing catalog** (adopt existing metadata.json) | ✅ | ✅ | A fresh pyiceberg catalog registered the newest metadata.json, read 3/3, then committed snapshot 4 in place. `exit_03_iceberg_new_catalog.py`, rows 3a/3b. |

Status vocabulary: ✅ works as tested · ❌ does not work as tested · ◑ partial · ❓ not isolated.

## What this means

- **The hypothesis held for the two catalog-free exits.** Both source formats carry a Delta log *and* Iceberg metadata, so the two starting points behaved identically in every row. Format is not the lock-in question; the catalog prefix is.
- **Iceberg is the clean no-copy exit.** Hand the newest `metadata.json` to any Iceberg catalog and it owns the table from there, including new commits into the same S3 prefix. UniForm writes it under `metadata/`, managed Iceberg under `_iceberg/metadata/`.
- **Delta-on-S3 works, but engine protocol support is uneven.** Default managed Delta uses reader 2/writer 7 with column mapping and IcebergCompatV2; managed Iceberg's Delta log adds v2 checkpoints and type widening. duckdb reads both; delta-rs 1.6 reads neither. That is a client capability gap, not a storage or catalog block.
- **You cannot re-adopt the same bytes as a UC external table.** The `__unitystorage/` prefix is reserved even after the managed table (or the whole catalog) is dropped. Staying in UC as *external* requires a copy; leaving UC entirely does not.
- **No-copy cutover is a hard cutover.** Once the new catalog commits, UC still points at the old snapshot. Freeze UC writes first (or drop the UC table) — the two catalogs will not reconcile.
- **Sequence the UC drop deliberately.** Dropping a managed table eventually deletes its files. In this run the files were still present after `DROP TABLE`, but a real migration should have the new catalog owning the table and consumers moved before the UC side is dropped.

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

## Migration guidance to evaluate after the capability matrix

1. **Profile interactions.** Inventory loading, transformation (including maintenance), and consumption separately.
2. **Parallel validation.** Reproduce loading and transformations in the destination, read-only against the source, until automated checks pass.
3. **Hard cutover.** Freeze source writes before advancing the destination catalog pointer. Move writes and transformations, leave only read-only source pointers, and continue validation.
4. **Migrate consumers in cohorts.** Prioritize strategic and expensive workloads, then scale old consumption infrastructure down as adoption moves through the long tail.

This guidance is a process hypothesis, not a measured matrix result.

## Safety

- OAuth/SSO profiles only; scripts refuse `dapi` personal access tokens.
- Real workspace URLs, identities, catalog names, and storage paths are scrubbed from committed results.
- Use a dedicated storage prefix. The experiment does not delete uncataloged object-storage files.
