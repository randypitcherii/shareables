"""
Bundle job entry point: deploy the model dispatcher.

Run by the `deploy_dispatcher` job in databricks.yml (a serverless
spark_python_task). It:
  1. creates the target schema if needed and points the MLflow experiment's
     traces at it (UC-backed tracing)
  2. logs serving_agent.py with code-based logging, declaring the four Unity AI
     Gateway model services it calls (luna, opus, argon, and JEV) as resources
  3. registers the model to UC as {catalog}.{schema}.simple_dispatcher_agent
  4. if deploy_endpoint == "true", deploys it with databricks.agents.deploy(...)
     and gives the endpoint a service-principal identity (see below)

Why the endpoint needs its own service principal: agents on Model Serving
"can't use automatic authentication passthrough to query model services"
(Databricks docs, "Migrate to Unity Gateway"). Declared resources still record
what the agent calls, but the credential that reaches /ai-gateway/* must be a
real principal with EXECUTE on the model services. So the endpoint gets
DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET from a secret scope, and the
agent code stays the same: the SDK picks OAuth M2M ahead of the
model-serving credential. That principal also writes the endpoint's MLflow
traces, so this job grants it access to the experiment and the trace schema.

Parameters arrive as command-line args (the bundle substitutes job variables):
    --catalog / --schema               UC home for the model and traces
    --luna-model / --opus-model / --argon-model / --jev-model   UC model ids
    --experiment                       MLflow experiment path
    --deploy-endpoint "true" | "false"
    --secret-scope                     scope holding the endpoint SP's client_id / client_secret
"""

from __future__ import annotations

import argparse
import os

import mlflow
from mlflow.models.resources import DatabricksUCModelService

MODEL_NAME = "simple_dispatcher_agent"

INPUT_EXAMPLE = {"input": [{"role": "user", "content": "Reply with the single word: ok"}]}


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
        "input_example": INPUT_EXAMPLE,
        "pip_requirements": ["mlflow>=3.0", "databricks-sdk", "requests"],
    }


def _parse_args(argv=None):
    p = argparse.ArgumentParser(description="Deploy the simple model dispatcher.")
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--luna-model", required=True, dest="luna_model")
    p.add_argument("--opus-model", required=True, dest="opus_model")
    p.add_argument("--argon-model", required=True, dest="argon_model")
    p.add_argument("--jev-model", required=True, dest="jev_model")
    p.add_argument("--experiment", required=True)
    p.add_argument("--deploy-endpoint", default="false", dest="deploy_endpoint")
    p.add_argument("--secret-scope", default="", dest="secret_scope")
    return p.parse_args(argv)


def agent_environment(args) -> dict[str, str]:
    """The env vars the dispatcher reads, derived from job parameters.

    Set locally before log_model (mlflow validates ResponsesAgent models by
    running predict on the input example, which makes one real routed call)
    and passed to agents.deploy so the serving container reads the same config.
    """
    return {
        "LUNA_MODEL": args.luna_model,
        "OPUS_MODEL": args.opus_model,
        "ARGON_MODEL": args.argon_model,
        "JEV_MODEL": args.jev_model,
    }


def serving_credentials_env(host: str, secret_scope: str) -> dict[str, str]:
    """OAuth M2M env vars for the endpoint, resolved by Model Serving from secrets."""
    return {
        "DATABRICKS_HOST": host,
        "DATABRICKS_CLIENT_ID": "{{secrets/" + secret_scope + "/client_id}}",
        "DATABRICKS_CLIENT_SECRET": "{{secrets/" + secret_scope + "/client_secret}}",
    }


def grant_trace_access(w, app_id: str, catalog: str, schema: str, experiment_id: str) -> None:
    """Let the endpoint's SP write traces: the experiment plus its UC trace tables."""
    from databricks.sdk.service.catalog import PermissionsChange, Privilege
    from databricks.sdk.service.ml import ExperimentAccessControlRequest, ExperimentPermissionLevel

    w.grants.update(
        "catalog", catalog, changes=[PermissionsChange(principal=app_id, add=[Privilege.USE_CATALOG])]
    )
    w.grants.update(
        "schema",
        f"{catalog}.{schema}",
        changes=[
            PermissionsChange(
                principal=app_id,
                add=[Privilege.USE_SCHEMA, Privilege.SELECT, Privilege.MODIFY],
            )
        ],
    )
    w.experiments.update_permissions(
        experiment_id,
        access_control_list=[
            ExperimentAccessControlRequest(
                service_principal_name=app_id,
                permission_level=ExperimentPermissionLevel.CAN_EDIT,
            )
        ],
    )


def model_resources(model_ids) -> list:
    """The Unity AI Gateway model services the serving identity may call.

    The deployed endpoint's credential is scoped to declared resources, so
    every model the dispatcher can route to (plus the router) is listed here.
    """
    return [DatabricksUCModelService(model_service_name=m) for m in dict.fromkeys(model_ids)]


def run(args) -> str:
    """Execute the full deploy flow. Returns the UC model name."""
    catalog, schema = args.catalog, args.schema
    ensure_schema(catalog, schema)  # dev/test schemas are per-user/per-PR

    env = agent_environment(args)
    os.environ.update(env)

    from agent import set_experiment_uc_backed

    set_experiment_uc_backed(args.experiment, catalog, schema)

    uc_model_name = f"{catalog}.{schema}.{MODEL_NAME}"
    mlflow.set_registry_uri("databricks-uc")
    resources = model_resources(env.values())

    with mlflow.start_run(run_name="log_dispatcher"):
        logged = mlflow.pyfunc.log_model(**log_model_kwargs(resources))
        registered = mlflow.register_model(model_uri=logged.model_uri, name=uc_model_name)
        print(f"Registered {uc_model_name} version {registered.version}")

    if str(args.deploy_endpoint).strip().lower() == "true":
        import base64

        from databricks import agents
        from databricks.sdk import WorkspaceClient

        if not args.secret_scope:
            raise ValueError(
                "--secret-scope is required to deploy: Model Serving can't pass its own "
                "credential through to Unity AI Gateway model services (see README)."
            )
        w = WorkspaceClient()
        app_id = base64.b64decode(
            w.secrets.get_secret(args.secret_scope, "client_id").value
        ).decode()
        experiment_id = mlflow.get_experiment_by_name(args.experiment).experiment_id
        grant_trace_access(w, app_id, catalog, schema, experiment_id)

        # NB: the parameter is `scale_to_zero`; `scale_to_zero_enabled` is
        # silently swallowed by **kwargs and the endpoint stays always-on.
        deployment = agents.deploy(
            uc_model_name,
            registered.version,
            scale_to_zero=True,
            environment_vars={**env, **serving_credentials_env(w.config.host, args.secret_scope)},
        )
        print(f"Deployed {uc_model_name} v{registered.version} → {deployment.endpoint_name}")
    else:
        print("deploy_endpoint != true; skipping endpoint deployment.")

    return uc_model_name


def ensure_schema(catalog: str, schema: str) -> None:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound

    w = WorkspaceClient()
    try:
        w.schemas.get(f"{catalog}.{schema}")
    except NotFound:
        w.schemas.create(name=schema, catalog_name=catalog)


if __name__ == "__main__":
    run(_parse_args())
