# Scripts

All driven through the Makefile (see `make help`); config loads from `${APP_ENV}.env`.

| Script | What it does |
|---|---|
| `00_generate_events.py` | Standalone producer: realistic ~1KB nested heterogeneous JSON with a ~65% noise mix. Env-configured only, so `make produce-remote` runs the same file ON the broker VM (inside the boot-built `pulsar-producer` container) for scale runs. |
| `01_run_databricks_cells.py` | Launches the DAB-deployed path A/B jobs via SDK `run_now` with a preset "cells" ladder (`scale-a`, `scale-b`, `ladder-a`, `ladder-b`), polls to completion. JSON params rule out `bundle run --params`. |
| `03_path_c_writer.py` | External writer (no Databricks compute): Pulsar consumer -> PyIceberg -> UC managed Iceberg over the Iceberg REST catalog, with filtering, timed windows, commit-interval control, and exact freshness capture. |
| `04_read_queries.py` | Clustered vs plain read benchmark (3 representative queries × 5 iterations, medians), file counts, timed `OPTIMIZE FULL`, post-optimize reads. |
| `05_cost_model.py` | Cost/perf model from measured results, extrapolated to 250k events/s; rates embedded. |
| `verify.py` | Collects Spark cell summaries, verifies MANAGED/format/VARIANT/row counts, computes exact freshness per ladder cell, filter reduction, and the labeled 15-min/hourly modeled cells. |

Prerequisites for path C (one-time, workspace admin): metastore external data
access enabled and `GRANT EXTERNAL USE SCHEMA` on the target schema.

Run order for the full matrix: `tf-apply` → `deploy-jobs` → `produce-remote`
(2M backlog) → `run-scale-a` / `run-scale-b` / `run-scale-c` /
`run-scale-c-filtered` → start `produce-remote-timed` (continuous) →
`run-ladder-a` / `run-ladder-b` / `run-ladder-c-*` → `run-reads` → `verify` →
`cost-model` → `tf-destroy`.
