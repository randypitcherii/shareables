"""
Bundle job entry point: deploy the model dispatcher.

Run by the `deploy_dispatcher` job in databricks.yml (a serverless
spark_python_task). It:
  1. creates the target schema if needed and points the MLflow experiment's
     traces at it (UC-backed tracing)
  2. logs serving_agent.py with code-based logging and an auth policy:
       - user policy: the `ai-gateway` scope, so model calls run as the caller
         (on-behalf-of-user). No secrets, no shared service principal.
       - system policy: the model services, listed for lineage
  3. registers the model to UC as {catalog}.{schema}.simple_dispatcher_agent
  4. if deploy_endpoint == "true", deploys it with databricks.agents.deploy(...),
     configuring endpoint telemetry to persist traces in the experiment's UC
     tables without secrets or grants to rotating serving principals

Why OBO: agents on Model Serving can't use automatic authentication
passthrough for Unity AI Gateway model services (the endpoint's own credential
gets `NOT_FOUND`), but the caller's down-scoped `ai-gateway` token works, and
it bills and permissions each request to the person who made it.

Parameters arrive as command-line args (the bundle substitutes job variables):
    --catalog / --schema               UC home for the model and traces
    --luna-model / --opus-model / --gemini-model / --jev-model   UC model ids
    --experiment                       MLflow experiment path
    --deploy-endpoint "true" | "false"
"""

from __future__ import annotations

import argparse
import os
from datetime import timedelta

import mlflow
from mlflow.models.auth_policy import AuthPolicy, SystemAuthPolicy, UserAuthPolicy
from mlflow.models.resources import DatabricksUCModelService

MODEL_NAME = "simple_dispatcher_agent"

INPUT_EXAMPLE = {"input": [{"role": "user", "content": "Reply with the single word: ok"}]}


# The Unity AI Gateway OAuth scope: the only one the dispatcher's user token needs.
USER_API_SCOPES = ["ai-gateway"]


def log_model_kwargs(auth_policy: AuthPolicy | None = None) -> dict:
    """The exact kwargs driver.py logs the model with.

    Shared with the packaging test so "what we test" and "what we deploy"
    cannot drift. code_paths must carry agent.py: serving_agent.py imports it,
    and without it the serving container fails with ModuleNotFoundError.
    """
    return {
        "name": MODEL_NAME,
        "python_model": "serving_agent.py",
        "code_paths": ["agent.py"],
        "auth_policy": auth_policy,
        "input_example": INPUT_EXAMPLE,
        "pip_requirements": ["mlflow==3.17.2", "databricks-sdk", "databricks-ai-bridge", "requests"],
    }


def _parse_args(argv=None):
    p = argparse.ArgumentParser(description="Deploy the simple model dispatcher.")
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--luna-model", required=True, dest="luna_model")
    p.add_argument("--opus-model", required=True, dest="opus_model")
    p.add_argument("--gemini-model", required=True, dest="gemini_model")
    p.add_argument("--jev-model", required=True, dest="jev_model")
    p.add_argument("--experiment", required=True)
    p.add_argument("--deploy-endpoint", default="false", dest="deploy_endpoint")
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
        "GEMINI_MODEL": args.gemini_model,
        "JEV_MODEL": args.jev_model,
    }


def endpoint_experiment_id(experiment: str) -> str:
    """Require the same UC-linked experiment for authoring and serving."""
    from mlflow.entities.trace_location import UnityCatalog

    found = mlflow.get_experiment_by_name(experiment)
    if found is None or not isinstance(found.trace_location, UnityCatalog):
        raise ValueError(
            f"Experiment {experiment!r} must be linked to Unity Catalog before deployment. "
            "Use a fresh experiment if the existing one contains workspace-managed traces."
        )
    return found.experiment_id


def configure_endpoint_telemetry(endpoint_name: str, experiment_id: str) -> None:
    """Bind managed endpoint telemetry to the experiment's existing UC tables.

    agents.deploy does not expose telemetry_config. Use its documented REST
    API through the SDK, including fields not yet exposed by older SDK models.
    The API requires logs, metrics, and spans table names even when only traces
    are enabled; its readback omits unused logs/metrics destinations.
    """
    from databricks.sdk import WorkspaceClient
    from mlflow.entities.trace_location import UnityCatalog

    location = mlflow.get_experiment(experiment_id).trace_location
    if not isinstance(location, UnityCatalog):
        raise ValueError(f"Experiment {experiment_id} is not linked to Unity Catalog.")
    table_names = {
        "traces_table": location.full_otel_spans_table_name,
        "logs_table": location.full_otel_logs_table_name,
        "metrics_table": f"{location.full_table_prefix}_otel_metrics",
        "annotations_table": (
            location.full_annotations_table_name or f"{location.full_table_prefix}_otel_annotations"
        ),
    }
    if not all(table_names.values()):
        raise ValueError(f"Experiment {experiment_id} has an incomplete UC trace location.")

    def matches(config: dict) -> bool:
        destinations = config.get("table_names") or {}
        return (
            all(destinations.get(key) == table_names[key] for key in ("traces_table", "annotations_table"))
            and set(config.get("enabled_telemetry_features") or []) == {"TELEMETRY_FEATURE_TRACES"}
        )

    workspace = WorkspaceClient()
    timeout = timedelta(minutes=30)
    workspace.serving_endpoints.wait_get_serving_endpoint_not_updating(endpoint_name, timeout=timeout)
    path = f"/api/2.0/serving-endpoints/{endpoint_name}"
    current = workspace.api_client.do("GET", path).get("telemetry_config") or {}
    if matches(current):
        print(f"UC endpoint telemetry already configured: {table_names['traces_table']}")
        return

    workspace.api_client.do(
        "PATCH",
        f"{path}/telemetry-config",
        body={"telemetry_config": {
            "table_names": table_names,
            "enabled_telemetry_features": ["TELEMETRY_FEATURE_TRACES"],
        }},
    )
    workspace.serving_endpoints.wait_get_serving_endpoint_not_updating(endpoint_name, timeout=timeout)
    configured = workspace.api_client.do("GET", path).get("telemetry_config") or {}
    if not matches(configured):
        raise RuntimeError(f"Endpoint {endpoint_name} did not retain its UC telemetry destination.")
    print(f"UC endpoint telemetry configured: {table_names['traces_table']}")


def auth_policy(model_ids) -> AuthPolicy:
    """Model calls run as the caller: a user policy with the `ai-gateway` scope.

    The model services sit in the system policy for lineage only. The
    endpoint's own credential can't call them; the caller's token does.
    """
    return AuthPolicy(
        system_auth_policy=SystemAuthPolicy(
            resources=[DatabricksUCModelService(model_service_name=m) for m in dict.fromkeys(model_ids)]
        ),
        user_auth_policy=UserAuthPolicy(api_scopes=USER_API_SCOPES),
    )


def run(args) -> str:
    """Execute the full deploy flow. Returns the UC model name."""
    mlflow.set_tracking_uri("databricks")
    catalog, schema = args.catalog, args.schema
    ensure_schema(catalog, schema)  # dev/test schemas are per-user/per-PR

    env = agent_environment(args)
    os.environ.update(env)

    from agent import set_experiment_uc_backed

    set_experiment_uc_backed(args.experiment, catalog, schema)
    experiment_id = endpoint_experiment_id(args.experiment)

    policy = auth_policy(env.values())

    uc_model_name = f"{catalog}.{schema}.{MODEL_NAME}"
    mlflow.set_registry_uri("databricks-uc")
    with mlflow.start_run(run_name="log_dispatcher"):
        logged = mlflow.pyfunc.log_model(**log_model_kwargs(policy))
        registered = mlflow.register_model(model_uri=logged.model_uri, name=uc_model_name)
        print(f"Registered {uc_model_name} version {registered.version}")

    if str(args.deploy_endpoint).strip().lower() == "true":
        from databricks import agents

        # NB: the parameter is `scale_to_zero`; `scale_to_zero_enabled` is
        # silently swallowed by **kwargs and the endpoint stays always-on.
        # MLFLOW_EXPERIMENT_ID is passed explicitly: agents.deploy otherwise
        # keeps the experiment of the endpoint's first deploy.
        deployment = agents.deploy(
            uc_model_name,
            registered.version,
            scale_to_zero=True,
            environment_vars={
                **env,
                "MLFLOW_EXPERIMENT_ID": experiment_id,
                "MLFLOW_ENABLE_OTLP_EXPORTER": "false",
            },
            tags={"MONITOR_EXPERIMENT_ID": experiment_id},
        )
        configure_endpoint_telemetry(deployment.endpoint_name, experiment_id)
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
