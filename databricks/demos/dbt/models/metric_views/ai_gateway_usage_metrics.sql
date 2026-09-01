{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/ai_gateway.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1
source: {{ source('system_ai_gateway', 'usage') }}
comment: AI Gateway inference requests (tokens, latency) with workspace context.
joins:
  - name: ws
    source: {{ source('system_access', 'workspaces_latest') }}
    "on": source.workspace_id = ws.workspace_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true
dimensions:
  - name: Event Time
    expr: source.event_time
    comment: The timestamp at which the request is received.
  - name: Event Date
    expr: CAST(source.event_time AS DATE)
    comment: Calendar date of the request.
    format:
      type: date
      date_format: year_month_day
  - name: Workspace ID
    expr: source.workspace_id
    comment: The workspace id of the AI Gateway endpoint.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Endpoint Name
    expr: source.endpoint_name
    comment: The name of the top level AI Gateway entity.
  - name: Destination Type
    expr: source.destination_type
    comment: The destination type.
  - name: Destination Name
    expr: source.destination_name
    comment: The name of the destination object (endpoint name or PPT UC model name).
  - name: Requester Type
    expr: source.requester_type
    comment: The type of the requester.
  - name: API Type
    expr: source.api_type
    comment: The API type of the request.
  - name: Status Code
    expr: source.status_code
    comment: The final HTTP status code returned by the endpoint.
measures:
  - name: Requests
    expr: COUNT(1)
    comment: Number of inference requests in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Total Input Tokens
    expr: SUM(source.input_tokens)
    comment: Total count of input tokens.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Total Output Tokens
    expr: SUM(source.output_tokens)
    comment: Total count of output tokens.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Total Tokens
    expr: SUM(source.total_tokens)
    comment: Sum of input and output tokens.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Avg Latency (ms)
    expr: AVG(source.latency_ms)
    comment: Average latency from request receipt to completion of proxy response, in ms.
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
  - name: Avg Time To First Byte (ms)
    expr: AVG(source.time_to_first_byte_ms)
    comment: Average latency from request receipt to first byte of the proxy response, in ms.
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
  - name: Distinct Endpoints
    expr: COUNT(DISTINCT source.endpoint_id)
    comment: Distinct number of AI Gateway endpoints in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

# ============================================================
# ai_gateway_external_model_spend_metrics  (source: system.ai_gateway.external_model_spend)
# ============================================================
