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
import os

import mlflow
from mlflow.models.resources import (
    DatabricksFunction,
    DatabricksGenieSpace,
    DatabricksServingEndpoint,
    DatabricksSQLWarehouse,
)

from register_web_search import FUNCTION_NAME, register_web_search

MODEL_NAME = "simple_dispatcher_agent"


def log_model_kwargs(resources: list | None = None) -> dict:
    """The exact kwargs driver.py logs the model with.

    Shared with the packaging test so "what we test" and "what we deploy"
    cannot drift. code_paths must carry agent.py: serving_agent.py imports it,
    and without it the serving container fails with ModuleNotFoundError.
    """
    return {
        "name": MODEL_NAME,
        "python_model": "serving_agent.py",
        "code_paths": ["agent.py"],
        "resources": resources or [],
        "pip_requirements": [
            "databricks-langchain",
            "langchain>=1.3",
            "mlflow>=3.0",
            "databricks-agents",
        ],
    }


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Deploy the simple dispatcher agent.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--genie-space-id", required=True, dest="genie_space_id")
    parser.add_argument("--llm-endpoint", required=True, dest="llm_endpoint")
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--deploy-endpoint", default="false", dest="deploy_endpoint")
    return parser.parse_args(argv)


def model_resources(genie_space_id, web_search_fqn, llm_endpoint, warehouse_id):
    """Everything the serving endpoint's identity needs access to.

    Declared at log time so agents.deploy can set up auth passthrough.
    """
    return [
        DatabricksGenieSpace(genie_space_id=genie_space_id),
        DatabricksSQLWarehouse(warehouse_id=warehouse_id),
        DatabricksFunction(function_name=web_search_fqn),
        DatabricksServingEndpoint(endpoint_name=llm_endpoint),
    ]


def _agent_environment(args) -> dict[str, str]:
    """The env vars the dispatcher needs, derived from job parameters.

    Set locally before log_model (mlflow validates ResponsesAgent models by
    running predict on an input example, which builds the dispatcher) and
    passed to agents.deploy so the serving container can build it too.
    """
    return {
        "GENIE_SPACE_ID": args.genie_space_id,
        "BASE_CATALOG": args.catalog,
        "BASE_SCHEMA": args.schema,
        "LLM_ENDPOINT": args.llm_endpoint,
    }


def run(args) -> str:
    """Execute the full deploy flow. Returns the UC model name."""
    catalog, schema = args.catalog, args.schema

    # 1. Register / refresh the web_search UDF in the target schema.
    register_web_search(catalog=catalog, schema=schema)

    # mlflow's log_model runs a validation predict on ResponsesAgent models,
    # which builds the dispatcher from these env vars — and makes one real
    # agent invocation, so a broken config fails the deploy job here instead
    # of at first user request.
    os.environ.update(_agent_environment(args))

    # serving_agent.py reads its config from the environment at load/serve time,
    # so the deployed model resolves GENIE_SPACE_ID / BASE_CATALOG / BASE_SCHEMA /
    # LLM_ENDPOINT the same way agent.py does. The endpoint's runtime identity
    # supplies the credentials; the declared resources below grant access.
    # UC-backed tracing: the experiment's traces land in Delta tables in the
    # target schema (created above) instead of workspace-managed storage.
    from agent import set_experiment_uc_backed

    set_experiment_uc_backed(args.experiment, catalog, schema)

    web_search_fqn = f"{catalog}.{schema}.{FUNCTION_NAME}"

    # The Genie space resource does NOT propagate access to the SQL warehouse
    # Genie executes on — the warehouse must be declared in its own right, so
    # derive it from the space instead of asking for more config.
    from databricks.sdk import WorkspaceClient

    warehouse_id = WorkspaceClient().genie.get_space(args.genie_space_id).warehouse_id
    resources = model_resources(
        genie_space_id=args.genie_space_id,
        web_search_fqn=web_search_fqn,
        llm_endpoint=args.llm_endpoint,
        warehouse_id=warehouse_id,
    )

    uc_model_name = f"{catalog}.{schema}.{MODEL_NAME}"
    mlflow.set_registry_uri("databricks-uc")

    # 2. Code-based logging: log serving_agent.py directly (it calls
    #    mlflow.models.set_model at import).
    with mlflow.start_run(run_name="log_dispatcher"):
        logged = mlflow.pyfunc.log_model(**log_model_kwargs(resources))

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
            environment_vars=_agent_environment(args),
        )
        print(f"Deployed {uc_model_name} v{registered.version} (scale-to-zero).")
    else:
        print("deploy_endpoint != true; skipping endpoint deployment.")

    return uc_model_name


if __name__ == "__main__":
    run(_parse_args())
