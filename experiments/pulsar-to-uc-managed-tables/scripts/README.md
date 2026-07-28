# Evaluation scripts

| Script | Path | What it does |
|---|---|---|
| `00_generate_events.py` | (shared) | Publishes synthetic JSON events to the topic with the native Pulsar client. One stream feeds every path (KoP `entryFormat=pulsar` makes it visible over both protocols). |
| `01` / `02` | A / B | Not laptop scripts — paths A and B are Databricks jobs defined in `../databricks/databricks.yml` (`make run-path-a`, `make run-path-b`). Sources in `../databricks/src/`. |
| `03_path_c_pyiceberg_rest.py` | C | Pulsar consumer → PyIceberg appends → UC **managed Iceberg** via the Iceberg REST catalog + credential vending. No Databricks compute in the ingest path. |
| `verify.py` | all | Confirms each landed table is MANAGED, right format, right row counts. Merges into `../results/matrix_results.json`. |
| `_common.py` | — | Env-file config loading, Databricks SDK helpers (profile OAuth, no PATs), SQL execution, results recording. |

All scripts run with `uv run python scripts/<name>.py` (or the make targets) and read
config from `${APP_ENV:-dev}.env` — see `../template.env`.
