"""Inference path A: load the UC model inside a dbt Python model.

The model is an MLflow model logged in Unity Catalog. dbt knows only its URI
(`models:/<catalog>.<schema>.<model>/<version>`, from the project vars). This
model builds no model and trains nothing. It loads a model version and applies
it to a table in batch.

How it runs: dbt submits this file as a serverless job run. `mapInPandas`
hands each Spark task an iterator of pandas batches. Each task loads the
model ONCE from Unity Catalog, then scores all of its batches. The compute
exists only while the dbt build runs, and there is no endpoint to keep up.

Why not `mlflow.pyfunc.spark_udf`: it expects one output value per input row.
This model (like many LLM-style pyfuncs) returns one OpenAI-style object per
BATCH, `{"data": [{"index": i, "embedding": [...]}, ...]}`. mapInPandas does
the same job as spark_udf and makes the per-row unpacking explicit.

The environment below must be able to load the model's flavor (here
`transformers` + `torch`). Pin it, so every build scores with the same code.
"""

import pandas as pd


def embed_partition(batches, model_uri):
    import mlflow

    # both explicit: MLflow 3 otherwise defaults to a local sqlite store
    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")
    model = mlflow.pyfunc.load_model(model_uri)  # once per task, not per batch

    for batch in batches:
        output = model.predict(pd.DataFrame({"input": batch["input_text"].tolist()}))
        vectors = [None] * len(batch)
        for item in output["data"]:
            vectors[item["index"]] = [float(x) for x in item["embedding"]]
        yield pd.DataFrame(
            {
                "input_id": batch["input_id"].to_numpy(),
                "input_text": batch["input_text"].to_numpy(),
                "embedding": vectors,
            }
        )


def model(dbt, session):
    # environment_key / environment_dependencies raise dbt-core's
    # CustomKeyInConfigDeprecation, but dbt-databricks 1.12 reads them ONLY
    # from the top-level config (not config.meta). The warning is expected.
    dbt.config(
        materialized="table",
        environment_key="ai_ml_inference",
        environment_dependencies=[
            "mlflow-skinny[databricks]==3.16.1",
            "transformers==4.57.6",
            "torch==2.14.0",
        ],
    )
    from pyspark.sql import functions as F

    model_uri = dbt.config.meta_get("ai_ml_inference_model_uri")
    inputs = dbt.ref("ai_ml_inference_input").select("input_id", "input_text")

    scored = inputs.repartition(4).mapInPandas(
        lambda batches: embed_partition(batches, model_uri),
        schema="input_id bigint, input_text string, embedding array<float>",
    )
    return scored.withColumn("model_uri", F.lit(model_uri))
