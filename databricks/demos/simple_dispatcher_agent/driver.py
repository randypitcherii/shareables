"""
Bundle job entry point: deploy the dispatcher agent.

Run by the `deploy_dispatcher` job in databricks.yml (a serverless
spark_python_task). It:
  1. registers/refreshes the web_search UC function into {catalog}.{schema}
  2. logs the agent via code-based logging from serving_agent.py, declaring the
     resources it needs for auth passthrough (Genie space, the web_search
     function, and the LLM serving endpoint)
  3. registers the model to UC as {catalog}.{schema}.simple_dispatcher_agent
  4. if deploy_endpoint == "true", deploys it with databricks.agents.deploy(...)

Parameters arrive as command-line args (the bundle substitutes job parameters
into the task `parameters` list):
    --catalog        UC catalog
    --schema         UC schema
    --genie-space-id Genie space ID
    --llm-endpoint   chat model serving endpoint
    --experiment     MLflow experiment path
    --deploy-endpoint "true" | "false"

This file runs as plain Python (no notebook magics) so it works as a
spark_python_task and is also importable/readable as a normal module.
"""

from __future__ import annotations

import argparse

import mlflow
from mlflow.models.resources import (
    DatabricksFunction,
    DatabricksGenieSpace,
    DatabricksServingEndpoint,
)

from register_web_search import FUNCTION_NAME, register_web_search

MODEL_NAME = "simple_dispatcher_agent"


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Deploy the simple dispatcher agent.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--genie-space-id", required=True, dest="genie_space_id")
    parser.add_argument("--llm-endpoint", required=True, dest="llm_endpoint")
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--deploy-endpoint", default="false", dest="deploy_endpoint")
    return parser.parse_args(argv)


def run(args) -> str:
    """Execute the full deploy flow. Returns the UC model name."""
    catalog, schema = args.catalog, args.schema

    # 1. Register / refresh the web_search UDF in the target schema.
    register_web_search(catalog=catalog, schema=schema)

    # serving_agent.py reads its config from the environment at load/serve time,
    # so the deployed model resolves GENIE_SPACE_ID / BASE_CATALOG / BASE_SCHEMA /
    # LLM_ENDPOINT the same way agent.py does. The endpoint's runtime identity
    # supplies the credentials; the declared resources below grant access.
    mlflow.set_experiment(args.experiment)

    web_search_fqn = f"{catalog}.{schema}.{FUNCTION_NAME}"
    resources = [
        DatabricksGenieSpace(genie_space_id=args.genie_space_id),
        DatabricksFunction(function_name=web_search_fqn),
        DatabricksServingEndpoint(endpoint_name=args.llm_endpoint),
    ]

    uc_model_name = f"{catalog}.{schema}.{MODEL_NAME}"
    mlflow.set_registry_uri("databricks-uc")

    # 2. Code-based logging: log serving_agent.py directly (it calls
    #    mlflow.models.set_model at import).
    with mlflow.start_run(run_name="log_dispatcher"):
        logged = mlflow.pyfunc.log_model(
            name=MODEL_NAME,
            python_model="serving_agent.py",
            resources=resources,
            pip_requirements=[
                "databricks-langchain",
                "langchain>=1.3",
                "mlflow>=3.0",
                "databricks-agents",
            ],
        )

        # 3. Register the model version to Unity Catalog.
        registered = mlflow.register_model(
            model_uri=logged.model_uri,
            name=uc_model_name,
        )
        print(f"Registered {uc_model_name} version {registered.version}")

    # 4. Optionally deploy a scale-to-zero serving endpoint.
    if str(args.deploy_endpoint).strip().lower() == "true":
        from databricks import agents

        agents.deploy(
            uc_model_name,
            registered.version,
            scale_to_zero_enabled=True,
        )
        print(f"Deployed {uc_model_name} v{registered.version} (scale-to-zero).")
    else:
        print("deploy_endpoint != true; skipping endpoint deployment.")

    return uc_model_name


if __name__ == "__main__":
    run(_parse_args())
