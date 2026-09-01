{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/query_history.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1

# Fact: system.query.history (one statement execution; statement_id globally unique).
# Inline SCD joins to warehouses and clusters via the compute struct, point-in-time
# on start_time. A statement uses either a warehouse or a cluster. Validated N:1.
source: {{ source('system_query', 'history') }}

comment: SQL / serverless query executions with performance metrics and point-in-time warehouse/cluster context.

joins:
  - name: ws
    source: {{ source('system_access', 'workspaces_latest') }}
    "on": source.workspace_id = ws.workspace_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  - name: warehouse
    source: |
      SELECT workspace_id, warehouse_id, warehouse_name, warehouse_type, warehouse_size, change_time,
             lead(change_time) OVER (PARTITION BY workspace_id, warehouse_id ORDER BY change_time) AS next_change_time
      FROM {{ source('system_compute', 'warehouses') }}
    "on": warehouse.workspace_id = source.workspace_id
      AND warehouse.warehouse_id = source.compute.warehouse_id
      AND source.start_time >= warehouse.change_time
      AND (source.start_time < warehouse.next_change_time OR warehouse.next_change_time IS NULL)
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  # FLAKE (2nd + 3rd level): cluster worker node hardware, plus the worker
  # instance pool -> pool node hardware (3rd level). See FLAKE_JOINS.md.
  - name: cluster
    source: |
      SELECT c.workspace_id, c.cluster_id, c.cluster_name, c.owned_by, c.worker_node_type,
             wn.core_count AS worker_core_count, wn.memory_mb AS worker_memory_mb,
             ip.instance_pool_name AS worker_pool_name, ipn.core_count AS worker_pool_core_count,
             c.change_time,
             lead(c.change_time) OVER (PARTITION BY c.workspace_id, c.cluster_id ORDER BY c.change_time) AS next_change_time
      FROM {{ source('system_compute', 'clusters') }} c
      LEFT JOIN {{ source('system_compute', 'node_types') }} wn ON wn.account_id = c.account_id AND wn.node_type = c.worker_node_type
      LEFT JOIN (
        SELECT workspace_id, instance_pool_id, instance_pool_name, node_type, change_time,
               lead(change_time) OVER (PARTITION BY workspace_id, instance_pool_id ORDER BY change_time) AS next_change_time
        FROM {{ source('system_compute', 'instance_pools') }}
      ) ip ON ip.workspace_id = c.workspace_id AND ip.instance_pool_id = c.worker_instance_pool_id
        AND c.change_time >= ip.change_time AND (c.change_time < ip.next_change_time OR ip.next_change_time IS NULL)
      LEFT JOIN {{ source('system_compute', 'node_types') }} ipn ON ipn.account_id = c.account_id AND ipn.node_type = ip.node_type
    "on": cluster.workspace_id = source.workspace_id
      AND cluster.cluster_id = source.compute.cluster_id
      AND source.start_time >= cluster.change_time
      AND (source.start_time < cluster.next_change_time OR cluster.next_change_time IS NULL)
    cardinality: many_to_one
    rely:
      at_most_one_match: true

dimensions:
  - name: Start Time
    expr: source.start_time
    comment: The time when Databricks received the request. Recorded in UTC.
  - name: Start Date
    expr: CAST(source.start_time AS DATE)
    comment: Calendar date the query was received.
    format:
      type: date
      date_format: year_month_day
  - name: Start Month
    expr: DATE_TRUNC('MONTH', source.start_time)
    comment: Month bucket of the query start for monthly trend analysis.
    format:
      type: date
      date_format: locale_short_month
  - name: Workspace ID
    expr: source.workspace_id
    comment: The ID of the workspace where the query was run.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Statement Type
    expr: source.statement_type
    comment: 'The statement type. For example: ALTER, COPY, INSERT.'
  - name: Execution Status
    expr: source.execution_status
    comment: 'The statement termination state. Possible values: FINISHED, FAILED, CANCELED.'
  - name: Executed By
    expr: source.executed_by
    comment: The email address or username of the user who ran the statement.
  - name: Compute Type
    expr: source.compute.type
    comment: The type of compute resource used to run the statement (WAREHOUSE or CLUSTER).
  - name: Warehouse Name
    expr: warehouse.warehouse_name
    comment: The name of the SQL warehouse.
  - name: Warehouse Size
    expr: warehouse.warehouse_size
    comment: The cluster size of the SQL warehouse (e.g. SMALL, MEDIUM, LARGE).
  - name: Cluster Name
    expr: cluster.cluster_name
    comment: User defined name for the cluster (for queries run on all-purpose clusters).
  - name: Cluster Worker Cores
    expr: cluster.worker_core_count
    comment: Number of vCPUs for the cluster worker node type (from node_types).
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Cluster Worker Pool Name
    expr: cluster.worker_pool_name
    comment: Name of the instance pool backing the cluster's workers, if any (3rd-level flake).
  - name: Cluster Worker Pool Cores
    expr: cluster.worker_pool_core_count
    comment: vCPUs of the cluster worker pool's node type (3rd-level flake instance_pools -> node_types).
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: From Result Cache
    expr: source.from_result_cache
    comment: TRUE indicates that the statement result was fetched from the cache.

measures:
  - name: Query Count
    expr: COUNT(1)
    comment: Number of statement executions in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Statements
    expr: COUNT(DISTINCT source.statement_id)
    comment: Distinct number of statement executions in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Failed Queries
    expr: COUNT(1) FILTER (WHERE source.execution_status = 'FAILED')
    comment: Number of statements that failed.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Users
    expr: COUNT(DISTINCT source.executed_by_user_id)
    comment: Distinct number of users who ran statements.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Avg Total Duration (ms)
    expr: AVG(source.total_duration_ms)
    comment: Average total execution time in milliseconds (excluding result fetch).
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
  - name: Total Duration (ms)
    expr: SUM(source.total_duration_ms)
    comment: Total execution time in milliseconds across the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Avg Execution Duration (ms)
    expr: AVG(source.execution_duration_ms)
    comment: Average time spent executing the statement in milliseconds.
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
  - name: Total Read Bytes
    expr: SUM(source.read_bytes)
    comment: Total bytes of files read after IO pruning.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Total Produced Rows
    expr: SUM(source.produced_rows)
    comment: Total number of rows returned by statements in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
