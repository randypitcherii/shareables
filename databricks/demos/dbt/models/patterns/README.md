# Migration patterns

Small, runnable examples of the things teams most often ask about when they move
Python-heavy pipelines into dbt on Databricks. Every example runs on **synthetic
data**, so a fresh clone builds with no access to real data.

```bash
make patterns                      # build + test every pattern (tag:patterns)
make train-segment-model ...       # MLflow step, outside dbt (see "Inference")
```

The folder is tagged `patterns`, **not** `daily`. The production schedule never
runs it. `tests/test_patterns.py` checks that the `daily` tag does not leak in.

## The lineage

```
synth_audience_members            synthetic raw source (dirty on purpose)
  └─ audience_members_validated   Python UDF + seed validation
       ├─ audience_region_mix     run-time dynamic SQL (pivot)
       ├─ audience_interest_embeddings           Python model, mapInPandas
       ├─ audience_interest_embeddings_ai_query  ai_query embeddings   [gated]
       └─ audience_features       compile-time dynamic SQL (one-hot)
            │
            ├──► scripts/train_segment_classifier.py   MLflow: train, register @champion
            │
            ├─ audience_segment_scores_udf       Spark UDF inference   [gated]
            └─ audience_segment_scores_serving   ai_query inference    [gated]
```

## The patterns

| Pattern | Where | Use it when |
| --- | --- | --- |
| **Python model** | `audience_interest_embeddings.py` | A transform is awkward in SQL: per-batch pandas/numpy, a library call, a local model. |
| **Decompose a Python pipeline without a rewrite** | `audience_interest_embeddings.py`, [docs/patterns/python-decomposition.md](../../docs/patterns/python-decomposition.md) | You have working pipeline code and want dbt lineage, tests, and environments around it. |
| **Python UDF** | `functions/patterns/normalize_region_code.py` | Row-level logic (alias maps, string cleanup) that SQL models call. dbt manages it like a model. |
| **Dynamic SQL (macro)** | `macros/patterns/one_hot.sql`, `audience_features.sql`, `audience_region_mix.sql` | You would otherwise hand-write one column per value. |
| **Seed-based validation** | `seeds/patterns/`, `audience_members_validated.sql`, `tests/assert_patterns_invalid_share_below_threshold.sql` | The set of valid values (timezones, region codes) must be reviewed and identical in every environment. |
| **Inference, MLflow as source of truth** | `audience_segment_scores_udf.py`, `audience_segment_scores_serving.sql`, `scripts/train_segment_classifier.py` | dbt scores rows with a model that MLflow trains, versions, and promotes. |

### Python model

A dbt Python model is a `model(dbt, session)` function that returns a Spark
DataFrame. dbt submits it as a **serverless** job run
(`+submission_method: serverless_cluster` in `dbt_project.yml`). There is no
cluster to manage, and extra packages go in `environment_dependencies`.

`mapInPandas` hands the function one pandas DataFrame per Arrow batch. So
pandas and numpy code runs unchanged, in parallel, with no Python row loop.

### Python UDF

Files in `functions/` are dbt resources (dbt 1.11+). dbt runs
`CREATE OR REPLACE FUNCTION ... LANGUAGE PYTHON`, puts the function in the
environment's schema (dev, CI, and prod, like a model), and SQL models call it
with `{{ function('normalize_region_code') }}(col)`. The file is the function
**body**: the declared arguments are in scope, and it ends with `return`.

Two rules come from how dbt ships the body. Never write two consecutive dollar
signs (they end the dollar-quoted body), and never write Jinja braces, even in
a comment. `tests/test_patterns.py` checks both, and unit-tests the logic
without a warehouse.

### Dynamic SQL: compile time vs run time

`one_hot(column, values, prefix, agg)` writes one column per value. What
matters is **when the value list is decided**:

- `audience_features.sql` uses a **compile-time** list (the
  `pattern_device_types` var). The column set is fixed and reviewable. Model
  features need this, because the model signature must not change when new
  data arrives.
- `audience_region_mix.sql` uses a **run-time** list
  (`dbt_utils.get_column_values`). A new region in the data becomes a new
  column with no code change. That is good for reports and wrong for features.

### Seed-based validation

The seeds hold the valid values: 519 IANA timezones and 249 ISO 3166-1
alpha-2 codes. Regenerate them with `make patterns-seeds`, which shows any
change as a CSV diff. Validation happens in three layers:

1. `audience_members_validated` **keeps every row**, flags it (`is_valid`),
   and NULLs the invalid value in a `_clean` column. Nothing is dropped
   silently.
2. A `severity: warn` test reports how many rows are invalid on every build.
   A **singular test** fails the build when the invalid share passes a
   threshold (`pattern_max_invalid_share`, default 0.25), which means
   something upstream broke.
3. `audience_features` keeps valid rows only, and `relationships` tests to the
   seeds are the hard gate: an invalid value can never reach a model.

The synthetic source is about 17% dirty by design: aliases (`UK`, `USA`,
`Germany`) that the UDF repairs, plus junk (`ZZ`, `??`) and bad timezones
(`US/Pacifc`, `EST5`) that validation catches.

### Inference: MLflow owns the model, dbt owns the data

```
dbt builds features  →  MLflow trains + registers @champion  →  dbt scores by model NAME
```

Training runs outside dbt, in `scripts/train_segment_classifier.py`. The
script reads `audience_features`, fits a logistic regression (in-segment vs
out-of-segment), and registers it in Unity Catalog with the `champion` alias.
dbt only knows the model name. To promote a retrained model, move the alias in
MLflow. dbt needs no change and no deploy.

There are two scoring paths for the same registered model:

| | Spark UDF (`audience_segment_scores_udf.py`) | Model Serving (`audience_segment_scores_serving.sql`) |
| --- | --- | --- |
| Runs on | the dbt Python model's serverless job run | a serving endpoint, called with `ai_query()` |
| Idle cost | none | none with scale-to-zero, but a cold start takes minutes |
| Good for | batch scoring in the pipeline | many consumers sharing one live model; pure-SQL pipelines |
| Enable with | `DBT_SEGMENT_MODEL_NAME=<catalog.schema.model>` | `DBT_SEGMENT_SERVING_ENDPOINT=<endpoint>` |

Both models are **disabled by default** because they need a model that exists
first. A clean clone stays green.

```bash
make patterns                     # 1. build the features
make train-segment-model \
  FEATURES_TABLE=<catalog>.<schema>.audience_features \
  MODEL_NAME=<catalog>.<schema>.audience_segment_classifier \
  WAREHOUSE_ID=<sql warehouse id> \
  SERVING_ENDPOINT=audience-segment-classifier   # 2. train; endpoint is optional
DBT_SEGMENT_MODEL_NAME=<catalog>.<schema>.audience_segment_classifier make patterns   # 3. score
```

The Spark-UDF model loads the pyfunc into its own environment
(`env_manager="local"`), so its `environment_dependencies` pins **must match**
the training script's pins.

The embeddings example has the same two-path shape:
`audience_interest_embeddings.py` computes vectors itself with `mapInPandas`
and bills compute time. `audience_interest_embeddings_ai_query.sql` calls a
Foundation Model embedding endpoint and bills per token. Enable it with
`DBT_EMBEDDING_ENDPOINT` (e.g. `databricks-gte-large-en`).

## Known limits

- dbt 1.12 warns `CustomKeyInConfigDeprecation` for `environment_key` and
  `environment_dependencies`. dbt-databricks reads them from the model config,
  so the warning is harmless.
- Python models cannot use Jinja or import repo modules on serverless. To
  share code between Python models, publish it as a wheel and list it in
  `environment_dependencies`.
