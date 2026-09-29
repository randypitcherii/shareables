# Decompose a Python pipeline into dbt, without a rewrite

**Problem.** A working Python pipeline (a notebook or a script) reads tables,
runs a few stages of pandas/Spark logic, and writes tables. It works, but it
has no lineage, no tests between stages, and no dev/CI/prod isolation. A
rewrite into SQL is slow and risky, and some stages don't fit SQL anyway.

**Approach.** Keep each stage's logic exactly as it is. Move only the **I/O**:
each stage becomes one dbt Python model, reads come in through `dbt.ref()`, and
the write becomes `return`. dbt then owns what the pipeline lacked:

- lineage between stages
- tests at each stage boundary
- schema routing per environment
- docs

The stage code is still the code that already passed review.

## Before: one monolith

```python
# legacy_pipeline.py -- one job, three stages, hard-coded tables
import hashlib
import numpy as np
import pandas as pd

EMBEDDING_DIM = 64

def clean_members(df):                        # stage 1
    return df.filter("region_code is not null").dropDuplicates(["member_id"])

def embed_texts(texts):                       # stage 2 (pure Python)
    vectors = np.zeros((len(texts), EMBEDDING_DIM), dtype=np.float32)
    ...
    return vectors

def embed_batches(batches):
    for batch in batches:
        yield pd.DataFrame({..., "embedding": list(embed_texts(batch["interests_text"].tolist()))})

members = clean_members(spark.read.table("prod.raw.audience_members"))
members.write.mode("overwrite").saveAsTable("prod.work.members_clean")

embedded = spark.read.table("prod.work.members_clean").mapInPandas(embed_batches, schema=...)
embedded.write.mode("overwrite").saveAsTable("prod.work.member_embeddings")
```

## After: one dbt model per stage

| Legacy | dbt |
| --- | --- |
| `spark.read.table("prod.work.members_clean")` | `dbt.ref("members_clean")` |
| `df.write.mode("overwrite").saveAsTable(...)` | `return df` (dbt materializes it) |
| hard-coded `prod.` names | environment routing by `generate_schema_name` |
| the stage functions | **copied unchanged** |

In this project, `models/patterns/audience_interest_embeddings.py` is stage 2
after the lift. `embed_texts` and `embed_batches` are the legacy functions, and
the only new code is `model()`:

```python
def model(dbt, session):
    dbt.config(materialized="table")
    members = dbt.ref("audience_members_validated").filter("is_valid").select("member_id", "interests_text")
    return members.mapInPandas(embed_batches, schema="member_id bigint, interests_text string, embedding array<float>")
```

A stage that is plain filtering or joining, like stage 1 above, is often
easier to maintain as a SQL model. Convert those when convenient, and not as a
condition of the migration.

## Rules that shape the lift

- **Copy the functions into the model file.** A serverless Python model cannot
  import modules from the repo, and a Python model cannot use Jinja. When
  several stages share helper code, publish the helpers as a wheel (to a UC
  volume or a package index) and list it in `environment_dependencies`, instead
  of copying.
- **One materialized table per stage boundary** that you want to test or
  reuse. If an intermediate result is only a stepping stone, keep it inside one
  model.
- **Add tests at each boundary** (`unique`, `not_null`, an expression on an
  array's size). These are the checks the monolith never had.
- **Pin dependencies per model** with `environment_key` and
  `environment_dependencies`, so each stage runs in the environment it was
  written for.
- **Move orchestration last.** When every stage is a model, one `dbt build`
  replaces the job's task graph, and the schedule selects a tag.
