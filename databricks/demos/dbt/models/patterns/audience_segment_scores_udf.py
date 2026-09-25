"""Inference, low-resource path: the registered MLflow model as a Spark UDF.

dbt holds only the model NAME (the `segment_model_name` var). The MLflow
registry decides WHICH version runs, through the `@champion` alias, so promoting
a retrained model is an alias move in MLflow. There is no dbt change and no
redeploy. Scoring runs in this model's own serverless job run, so no endpoint
stays up between builds.

DISABLED unless `segment_model_name` (DBT_SEGMENT_MODEL_NAME) is set, because
the model has to exist first: run scripts/train_segment_classifier.py.

The environment pins below must match the training script's pins. A pyfunc
loaded with env_manager="local" runs in THIS environment.
"""

# keep in sync with scripts/train_segment_classifier.py
FEATURES = [
    "sessions_30d",
    "avg_watch_minutes",
    "days_since_signup",
    "device_ctv",
    "device_mobile",
    "device_desktop",
    "device_tablet",
]


def model(dbt, session):
    # environment_key / environment_dependencies raise dbt-core's
    # CustomKeyInConfigDeprecation, but dbt-databricks 1.12 reads them ONLY from
    # the top-level config (not config.meta). Moving them would silently drop
    # the pins, so the warning is expected here.
    dbt.config(
        materialized="table",
        environment_key="segment_scoring",
        environment_dependencies=[
            "mlflow-skinny[databricks]==3.16.1",
            "scikit-learn==1.9.1",
            "skops==0.15.0",
        ],
    )
    import mlflow
    from pyspark.sql import functions as F

    model_name = dbt.config.meta_get("segment_model_name")
    mlflow.set_registry_uri("databricks-uc")
    predict_proba = mlflow.pyfunc.spark_udf(
        session,
        model_uri=f"models:/{model_name}@champion",
        result_type="array<double>",
        env_manager="local",
    )

    features = dbt.ref("audience_features")
    scored = features.withColumn(
        "_proba", predict_proba(*[F.col(c).cast("double") for c in FEATURES])
    )
    return scored.select(
        "member_id",
        F.col("_proba")[1].alias("segment_probability"),
        (F.col("_proba")[1] >= 0.5).alias("predicted_in_segment"),
        "in_segment",
        F.lit(model_name).alias("model_name"),
    )
