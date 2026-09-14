# Experiment design — 2026-09-14

## Question

Which Glue-registered Iceberg tables with complex columns can Unity Catalog
read through Glue federation, and does the answer depend on which Iceberg
catalog implementation wrote the Glue StorageDescriptor?

## Variables

- **Writer path** (2): Glue Iceberg REST endpoint (`rest`) vs pyiceberg
  `GlueCatalog` (`glue`). Same S3 warehouse, same Glue database.
- **Type shape** (6): primitives, `list<prim>`, `list<struct>`, `map`,
  `struct<list>`, nested struct with required fields.
- **Read surface** (8): DESCRIBE variants, SELECT variants, REFRESH FOREIGN
  TABLE, information_schema, SHOW CREATE TABLE.

## Held constant

- Foreign catalog with `storage_root` set and `authorized_paths` covering the
  warehouse — removes the known `browse_only` failure mode as a confounder.
- Serverless SQL warehouse as the reader (Databricks SQL channel). Classic
  compute is out of scope for the first run; add a `RESULT_KEY_SUFFIX` rerun
  if a DBR-version dependence is suspected.
- pyiceberg as the client for both writers, so client-side type mapping is
  identical and only the catalog implementation differs.

## Not attempted

- Running an actual Flink job. The REST endpoint sets the SD server-side, so
  any REST client reproduces the writer path; Flink adds cost and no signal.
- Fixing federation. Out of scope by design.

## Fail-closed choices

- Row 1 must pass before rows 2–8 mean anything; the Makefile's `run-core`
  orders it first.
- `record_result` refuses rows without a proven identity and redacts on write.
- `probe_uc_read` retries only on `TABLE_OR_VIEW_NOT_FOUND` (sync lag), so a
  slow crawl cannot be mistaken for a type failure — and a type failure is
  never retried into looking flaky.
