# UC tracing from Model Serving without secrets

## Use managed endpoint telemetry, not deployment-principal grants

`agents.deploy` enables MLflow tracing, but that alone did not authorize its
rotating system identity to write a UC-linked experiment. Configuring the
endpoint's **managed telemetry destination** to the experiment's existing OTel
tables provides a secret-free persistence path. Model calls remain OBO with
exactly `UserAuthPolicy(api_scopes=["ai-gateway"])`.

The relevant supported interfaces are the
[endpoint telemetry guide](https://docs.databricks.com/aws/en/machine-learning/model-serving/custom-model-serving-uc-logs)
and the [telemetry configuration API](https://docs.databricks.com/api/model-serving/v1/patch-inference-endpoint-telemetry-config).
This is a composition of those APIs with `agents.deploy`, not a tracing-resource
argument exposed by `agents.deploy` itself.

## Repeatable deployment

1. Create or reuse an experiment bound with
   `mlflow.set_experiment(..., trace_location=UnityCatalog(...))` **before**
   deployment. The deploy job rejects a workspace-managed experiment rather
   than silently creating an `_endpoint` sibling.
2. Deploy with the experiment's `MLFLOW_EXPERIMENT_ID` and
   `MLFLOW_ENABLE_OTLP_EXPORTER=false`. Native MLflow tracing remains enabled;
   only the extra raw OTLP exporter is disabled.
3. After `agents.deploy`, wait for the endpoint update, then PATCH
   `/api/2.0/serving-endpoints/<endpoint>/telemetry-config` with the experiment's
   spans, logs, metrics, and annotations table names and
   `enabled_telemetry_features=["TELEMETRY_FEATURE_TRACES"]`.
4. Wait for the telemetry rollout and verify its readback. On later deploys,
   matching telemetry is retained, so no second update is needed.

`driver.configure_endpoint_telemetry` implements steps 3–4. The API requires all
three logs/spans/metrics names even when only traces are enabled; its readback
omits the unused logs/metrics destinations. The comparison deliberately checks
the active spans/annotations destinations, not equality with the entire request.

Run the existing bundle commands; there is no separate grant or secret step:

```bash
databricks bundle deploy -t dev --var deploy_endpoint=true
databricks bundle run deploy_dispatcher -t dev --var deploy_endpoint=true
```

Verify writes with the `jev_router` SQL query in the [README](README.md).
To reconstruct a complete trace, use an authorized local CLI profile and an
existing SQL warehouse for UC trace reads:

```python
import os

import mlflow

os.environ["DATABRICKS_CONFIG_PROFILE"] = "<profile>"
os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = "<warehouse-id>"
mlflow.set_tracking_uri("databricks")
trace = mlflow.get_trace("trace:/<catalog>.<schema>.<experiment-id>/<trace-id>")
print([span.name for span in trace.data.spans])
print(trace.info.tags["dispatcher.route"])
```

The table prefix in the URI is the experiment's `trace_location.full_table_prefix`;
this demo uses the experiment ID. The SQL warehouse is for querying traces,
not a credential or a dependency of endpoint-side export.

**User cost:** no PAT, client secret, user-managed service principal, or per-deploy UC
grant. The deployer must already have the documented UC creation/write
permissions. The first telemetry configuration causes an additional endpoint
rollout. Table-name telemetry fields are marked **Public Preview**; availability
is regional. No workspace preview setting is changed by this demo.

## Evidence and version boundary

The investigation uses MLflow **3.17.2**, databricks-agents **1.13.0**, and
databricks-sdk **0.158.0**. MLflow is pinned so model packaging, offline tests,
and the bundle job use the same version.

Live probe evidence on **November 1, 2026**:

- A throwaway agent's spans landed in the existing `_otel_spans` table of its
  UC-linked experiment after configuring endpoint telemetry. No explicit UC
  grants or secrets were added; catalog/schema grant snapshots were unchanged.
- Redeploying that probe through `agents.deploy` retained its telemetry
  configuration. With the extra OTLP exporter disabled, one request produced
  exactly **two rows, two distinct span IDs, and one trace ID** (`predict` and
  `uctrace_probe_router`). `mlflow.get_trace("trace:/<catalog>.<schema>.<prefix>/<trace-id>")`
  reconstructed both spans and the root's `probe.marker` tag. This verifies
  native MLflow trace persistence, not just raw OTel ingestion.
- Enabling **both** native tracing and raw OTLP dual export produced duplicate
  span IDs, with differently encoded attributes. The demo disables the extra
  OTLP exporter rather than sending two copies.
- The bundle deployed **dev model v9** successfully. Its telemetry helper
  reported `UC endpoint telemetry already configured`, demonstrating that the
  existing destination survived the real demo redeployment as well.
- Three unforced requests selected **luna, opus, and gemini** with
  `decided_by=jev`. SQL returned exactly **three distinct `jev_router` spans
  across three traces** in the dev experiment's UC spans table.
  `mlflow.get_trace` reconstructed `predict`, `dispatch`, `jev_router`, and
  `call_model` for each, with the matching `dispatcher.route` tag. The deployed
  user policy still contains only `ai-gateway`, with no client credentials.

The three probe endpoints, the probe registered model and its three versions,
and seven probe UC objects (trace tables, payload table, and unified view) were
deleted afterward. The probe experiment was soft-deleted. Catalog/schema
grants remained unchanged. Two pre-existing, zero-traffic dev serving entries
referenced deleted secrets and blocked deployment; only those entries were
removed, leaving registered model history intact.

An optional-exporter compatibility observation, outside the selected path:

- A separate local raw-OTLP test found that MLflow 3.17.2 plus OTel SDK 1.45.1
  can drop a tagged root with `Cannot mutate immutable BoundedAttributes`.
  OTel 1.38.0 passed that local test. The recommended native path does not add
  or pin this optional exporter and does not modify agent instrumentation.

## Alternatives and why they are not the default

| Candidate | Evidence / disposition | User-facing cost |
| --- | --- | --- |
| Managed endpoint telemetry | Live UC ingestion succeeded; implemented with the documented REST API | Existing UC permissions; first rollout; Public Preview table-name fields |
| Stable system principal or narrow group | No documented stable per-endpoint identity/group found in docs, Glean, or installed deployment APIs; no broad group was granted access | A broad `users` grant would expose trace data and is not acceptable |
| `DatabricksTable` system resources | Already established insufficient; installed MLflow resource API has no trace-destination/write-privilege resource | Does not eliminate per-deploy authorization repair |
| Manual grants to each system principal | Already established to work for one deployment, but the principal rotates | Per-deploy discovery/grants; cleanup; avoid brittle log parsing |
| Destination environment variables | Installed `tracing/destination.py` routes traces but does not change credentials; table-prefix form is explicitly unsupported in `MLFLOW_TRACING_DESTINATION` | No authorization benefit |
| More OBO user scopes | Installed MLflow exporter resolves its own default SDK credentials; adding scopes alone does not switch it to the request's `ModelServingUserCredentials` | Broader caller scopes and a custom request-aware exporter; not changed or live-tested |
| Dedicated OAuth M2M exporter | Documented by the UC tracing guide, but unnecessary after the managed path worked; not provisioned/tested | Stable SP grants plus a managed client secret and rotation; `DATABRICKS_*` are not tracing-only settings |
| Repoint the experiment | A destination is not an auth mechanism; retain the existing UC binding. Trace-bearing managed experiments need a fresh UC-linked experiment | Migration/new experiment; no fix for serving identity |
| Databricks Apps / Agent Server | Documented alternative with an app identity and declared UC resources; no migration needed here | Different deployment/runtime and auth integration; not live-tested |

Source inspection: installed `databricks/agents/deployments.py` sets tracing env
vars and creates/updates endpoints, but exposes no telemetry configuration
parameter. Installed `mlflow/models/resources.py` and `auth_policy.py` describe
the available resources; `tracing/provider.py`, `tracing/destination.py`, and
`utils/databricks_utils.py` separate destination selection from authentication.

## Documentation boundaries

- [Store OpenTelemetry traces in Unity Catalog](https://docs.databricks.com/aws/en/mlflow3/genai/tracing/trace-unity-catalog)
  documents direct exporter OAuth credentials and explicit UC privileges. It
  does not say that setting an experiment ID alone grants table access.
- [Deploy on Model Serving](https://docs.databricks.com/aws/en/agents/custom-agents/model-serving/deploy-agent)
  documents automatic agent tracing, not an `agents.deploy` telemetry argument.
- [Production monitoring](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/production-monitoring)
  is a separate scorer workflow. UC-backed monitoring jobs also need a persisted
  SQL warehouse ID on the experiment; enabling trace persistence does not
  register scorers or configure those jobs.
