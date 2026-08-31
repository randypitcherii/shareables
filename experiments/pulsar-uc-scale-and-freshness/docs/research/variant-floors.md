# VARIANT landing: what works today, and the version floors

The target workload's payloads are complex, nested, heterogeneous JSON (arrays of
objects, optional keys, mixed types) that the team wants queryable as VARIANT —
their stated pain is exactly the "analytics on nested values are unsupported or
slow/expensive" failure mode.

## Delta paths (A/B): VARIANT lands today, GA

`try_parse_json(raw)` in the streaming parse writes a real VARIANT column into the
managed Delta table. VARIANT on Delta is GA (DBR 15.3+; this rig runs 16.4).
`verify.py` asserts `information_schema.columns` reports `VARIANT` for the `event`
column on every Delta cell, and the clustering read benchmark queries it with
`event:path.to.field::TYPE` syntax.

Layout caveat that shapes the whole table design: a VARIANT column cannot be a
clustering/partition key, carries no min/max stats (no data skipping), and cannot
appear in GROUP BY/ORDER BY/DISTINCT directly. **Promoted typed columns carry the
layout**: this experiment promotes `event_id`, `seq`, `event_ts`, `event_type`,
`project_id` and clusters on those, keeping the VARIANT for ad-hoc depth. This is
the pattern to recommend, not an implementation detail.

## Iceberg path (C): JSON string today, VARIANT is version-gated

- Iceberg VARIANT is a **v3** feature. Writers need an Iceberg library recent
  enough to carry the shredding/metadata fixes (apache/iceberg#14655 landed the
  relevant bug fix); readers of v3 VARIANT need a DBR-18-class engine.
- **PyIceberg cannot write VARIANT today** (0.9/0.10 line). So path C (and any
  external writer built on PyIceberg) lands the payload as a JSON string and a
  silver hop converts to VARIANT (`parse_json` on Databricks compute, where it is
  GA). That silver hop exists anyway in most designs (dedup, casting, filtering
  refinement), so the floor costs a stage that was already planned — but it means
  the *bronze* Iceberg table does not serve VARIANT-shaped queries directly.
- Vendor broker-side Iceberg writers advertise variant-type support behind the same
  Iceberg v3 + fixed-library floor; confirm the vendor's library version and the
  reading DBR before counting on it.

## Bottom line for a GA-only estate

Want VARIANT in bronze **now** → the landing table must be managed Delta written by
Databricks compute (path A under a GA-only gate). Want zero-Databricks-compute
ingest **now** → accept JSON-string bronze + silver VARIANT conversion (path C).
The two constraints cannot be satisfied in one table until the Iceberg v3 VARIANT
floor clears end-to-end (writer library + reader DBR).
