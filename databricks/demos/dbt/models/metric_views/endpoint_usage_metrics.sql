{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/endpoint_usage.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1

# Fact: system.serving.endpoint_usage (one model serving request token record).
# Inline SCD served_entities join via served_entity_id (documented safe FK),
# point-in-time on request_time. Validated N:1.
source: {{ source('system_serving', 'endpoint_usage') }}

comment: Model serving request token usage with point-in-time served-entity/endpoint context.

joins:
  - name: ws
    source: {{ source('system_access', 'workspaces_latest') }}
    "on": source.workspace_id = ws.workspace_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  - name: served
    source: |
      SELECT workspace_id, served_entity_id, endpoint_name, served_entity_name, entity_type, entity_name, entity_version, change_time,
             lead(change_time) OVER (PARTITION BY workspace_id, served_entity_id ORDER BY change_time) AS next_change_time
      FROM {{ source('system_serving', 'served_entities') }}
    "on": served.workspace_id = source.workspace_id
      AND served.served_entity_id = source.served_entity_id
      AND source.request_time >= served.change_time
      AND (source.request_time < served.next_change_time OR served.next_change_time IS NULL)
    cardinality: many_to_one
    rely:
      at_most_one_match: true

dimensions:
  - name: Request Time
    expr: source.request_time
    comment: The timestamp at which the request is received.
  - name: Request Date
    expr: CAST(source.request_time AS DATE)
    comment: Calendar date of the request.
    format:
      type: date
      date_format: year_month_day
  - name: Workspace ID
    expr: source.workspace_id
    comment: The workspace ID for the workspace in which the serving endpoint exists.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Requester
    expr: source.requester
    comment: The name or email of the user or service principal whose permissions are used for the invocation request.
  - name: Status Code
    expr: source.status_code
    comment: The HTTP status code that was returned from the model.
  - name: Streaming
    expr: source.request_streaming
    comment: Whether the request is in stream mode.
  - name: Endpoint Name
    expr: served.endpoint_name
    comment: The name of the serving endpoint.
  - name: Served Entity Name
    expr: served.served_entity_name
    comment: The name of the served entity.
  - name: Entity Type
    expr: served.entity_type
    comment: Type of the entity that is served (FEATURE_SPEC, EXTERNAL_MODEL, FOUNDATION_MODEL, CUSTOM_MODEL).
  - name: Entity Name
    expr: served.entity_name
    comment: The underlying name of the entity (e.g. the Unity Catalog model name).

measures:
  - name: Requests
    expr: COUNT(1)
    comment: Number of serving requests in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Total Input Tokens
    expr: SUM(source.input_token_count)
    comment: Total input token count across requests.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Total Output Tokens
    expr: SUM(source.output_token_count)
    comment: Total output token count across requests.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Avg Input Tokens
    expr: AVG(source.input_token_count)
    comment: Average input token count per request.
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
  - name: Avg Output Tokens
    expr: AVG(source.output_token_count)
    comment: Average output token count per request.
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
  - name: Distinct Requesters
    expr: COUNT(DISTINCT source.requester)
    comment: Distinct number of requesters in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
