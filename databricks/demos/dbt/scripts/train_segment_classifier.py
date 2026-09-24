# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = [
#     "databricks-sdk>=0.40",
#     "mlflow-skinny[databricks]==3.16.1",
#     "pandas>=2.2",
#     "scikit-learn==1.9.1",
#     "skops==0.15.0",
# ]
# ///
"""Train the synthetic segment classifier and register it in Unity Catalog.

This step runs OUTSIDE dbt on purpose. It follows the lifecycle the patterns
examples model:

    ingestion  ->  dbt (features)  ->  MLflow (train + registry)  ->  dbt (inference)

dbt builds `audience_features`. This script reads that table, fits a logistic
regression (in-segment vs out-of-segment), logs it to MLflow, and registers it
as a Unity Catalog model with the `champion` alias. The scoring models then read
the model back BY NAME, so the MLflow registry stays the single source of truth
for which model version is live. Promote a new version by moving the alias. No
dbt change is needed.

Usage (from databricks/demos/dbt/, after `make patterns`):

    uv run scripts/train_segment_classifier.py \\
        --features-table my_catalog.my_schema.audience_features \\
        --model-name my_catalog.my_schema.audience_segment_classifier \\
        --warehouse-id <sql warehouse id>

    # optional: also serve it, for the ai_query inference path
    uv run scripts/train_segment_classifier.py ... --serving-endpoint audience-segment-classifier

Auth is the standard Databricks unified auth chain (e.g. DATABRICKS_CONFIG_PROFILE
or `databricks auth login`). No token is ever stored.
"""

from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

# Keep this list in sync with the scoring models:
# models/patterns/audience_segment_scores_udf.py and ..._serving.sql
FEATURES = [
    "sessions_30d",
    "avg_watch_minutes",
    "days_since_signup",
    "device_ctv",
    "device_mobile",
    "device_desktop",
    "device_tablet",
]
LABEL = "in_segment"
ALIAS = "champion"
# The environment the model runs in once served. Explicit, so MLflow never
# infers it from whatever happens to be installed where training ran. These
# pins must match the scoring model's environment_dependencies.
SERVING_REQUIREMENTS = ["scikit-learn==1.9.1", "skops==0.15.0"]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--features-table", required=True, help="catalog.schema.audience_features"
    )
    parser.add_argument(
        "--model-name", required=True, help="UC model name: catalog.schema.model"
    )
    parser.add_argument(
        "--warehouse-id", required=True, help="SQL warehouse used to read the features"
    )
    parser.add_argument(
        "--experiment",
        default=None,
        help="MLflow experiment path (default: /Users/<you>/audience_segment_classifier)",
    )
    parser.add_argument(
        "--serving-endpoint",
        default=None,
        help="also create/update a scale-to-zero Model Serving endpoint with this name",
    )
    return parser.parse_args(argv)


def read_features(client, warehouse_id: str, table: str) -> pd.DataFrame:
    """Read the dbt-built feature table through the SQL Statement Execution API."""
    from databricks.sdk.service.sql import Disposition, Format, StatementState

    columns = ", ".join(["member_id", *FEATURES, LABEL])
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=f"select {columns} from {table}",
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        response = client.statement_execution.get_statement(response.statement_id)
    if response.status.state != StatementState.SUCCEEDED:
        sys.exit(f"reading {table} failed: {response.status.error}")

    rows = list(response.result.data_array or [])
    chunk = response.result
    while chunk.next_chunk_index is not None:
        chunk = client.statement_execution.get_statement_result_chunk_n(
            response.statement_id, chunk.next_chunk_index
        )
        rows.extend(chunk.data_array or [])

    frame = pd.DataFrame(rows, columns=["member_id", *FEATURES, LABEL])
    frame[FEATURES] = frame[FEATURES].astype(float)
    frame[LABEL] = frame[LABEL].map({"true": 1, "false": 0}).astype(int)
    return frame


def train(frame: pd.DataFrame):
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score
    from sklearn.model_selection import train_test_split
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    x_train, x_test, y_train, y_test = train_test_split(
        frame[FEATURES],
        frame[LABEL],
        test_size=0.25,
        random_state=42,
        stratify=frame[LABEL],
    )
    model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000))
    model.fit(x_train, y_train)
    auc = roc_auc_score(y_test, model.predict_proba(x_test)[:, 1])
    return (
        model,
        x_train,
        {
            "test_auc": float(auc),
            "train_rows": len(x_train),
            "positive_rate": float(frame[LABEL].mean()),
        },
    )


def register(
    model, x_train: pd.DataFrame, metrics: dict, model_name: str, experiment: str
) -> str:
    import mlflow
    from mlflow.models import infer_signature

    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(experiment)

    # predict_proba, so the scoring models get P(in_segment) rather than a hard
    # 0/1 label; the threshold is a business decision made downstream in SQL
    sample = x_train.head(5)
    signature = infer_signature(sample, model.predict_proba(sample))
    with mlflow.start_run(run_name="audience_segment_classifier"):
        mlflow.log_metrics(metrics)
        info = mlflow.sklearn.log_model(
            model,
            name="model",
            signature=signature,
            input_example=sample,
            pyfunc_predict_fn="predict_proba",
            pip_requirements=SERVING_REQUIREMENTS,
            registered_model_name=model_name,
        )
    version = str(info.registered_model_version)
    mlflow.MlflowClient().set_registered_model_alias(model_name, ALIAS, version)
    return version


def serve(client, endpoint: str, model_name: str, version: str) -> None:
    from databricks.sdk.service.serving import (
        EndpointCoreConfigInput,
        ServedEntityInput,
    )

    entity = ServedEntityInput(
        entity_name=model_name,
        entity_version=version,
        workload_size="Small",
        scale_to_zero_enabled=True,
    )
    existing = {e.name for e in client.serving_endpoints.list()}
    if endpoint in existing:
        client.serving_endpoints.update_config(name=endpoint, served_entities=[entity])
        print(
            f"updating serving endpoint {endpoint} -> {model_name} v{version} (not waiting)"
        )
    else:
        client.serving_endpoints.create(
            name=endpoint,
            config=EndpointCoreConfigInput(name=endpoint, served_entities=[entity]),
        )
        print(
            f"creating serving endpoint {endpoint} -> {model_name} v{version} (not waiting)"
        )


def main(argv: list[str]) -> int:
    from databricks.sdk import WorkspaceClient

    args = parse_args(argv)
    client = WorkspaceClient()
    experiment = (
        args.experiment
        or f"/Users/{client.current_user.me().user_name}/audience_segment_classifier"
    )

    frame = read_features(client, args.warehouse_id, args.features_table)
    print(f"read {len(frame)} rows from {args.features_table}")
    model, x_train, metrics = train(frame)
    print(f"metrics: {metrics}")
    version = register(model, x_train, metrics, args.model_name, experiment)
    print(f"registered {args.model_name} v{version} with alias @{ALIAS}")

    if args.serving_endpoint:
        serve(client, args.serving_endpoint, args.model_name, version)

    print(
        "\nscore it with dbt:\n"
        f"  DBT_SEGMENT_MODEL_NAME={args.model_name} make patterns"
        + (
            f"\n  DBT_SEGMENT_SERVING_ENDPOINT={args.serving_endpoint} make patterns"
            if args.serving_endpoint
            else ""
        )
    )
    return 0


if __name__ == "__main__":
    os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")
    sys.exit(main(sys.argv[1:]))
