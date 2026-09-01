{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/node_timeline.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1

# Fact: system.compute.node_timeline (one node-minute utilization record).
# Grain (workspace_id, cluster_id, instance_id, start_time). Inline SCD cluster
# join (point-in-time on start_time) + node_types lookup. Validated N:1.
source: {{ source('system_compute', 'node_timeline') }}

comment: Compute node utilization (CPU/memory/network) with point-in-time cluster and node-type context.

joins:
  - name: ws
    source: {{ source('system_access', 'workspaces_latest') }}
    "on": source.workspace_id = ws.workspace_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  # FLAKE (2nd + 3rd level): node_types pre-joined for the cluster's configured
  # worker hardware, AND the cluster's worker instance pool -> that pool's node
  # hardware (3rd level, point-in-time on the cluster's change_time). All joins
  # are N:1 and pre-joined inside this source so the cluster stays one row per
  # version (0 inflation verified). See FLAKE_JOINS.md.
  - name: cluster
    source: |
      SELECT c.workspace_id, c.cluster_id, c.cluster_name, c.owned_by, c.dbr_version, c.data_security_mode,
             c.worker_node_type,
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
      AND cluster.cluster_id = source.cluster_id
      AND source.start_time >= cluster.change_time
      AND (source.start_time < cluster.next_change_time OR cluster.next_change_time IS NULL)
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  - name: node
    source: {{ source('system_compute', 'node_types') }}
    "on": node.account_id = source.account_id AND node.node_type = source.node_type
    cardinality: many_to_one
    rely:
      at_most_one_match: true

dimensions:
  - name: Start Time
    expr: source.start_time
    comment: Start time for the record in UTC.
  - name: Start Date
    expr: CAST(source.start_time AS DATE)
    comment: Calendar date of the utilization record.
    format:
      type: date
      date_format: year_month_day
  - name: Workspace ID
    expr: source.workspace_id
    comment: ID of the workspace where this compute resource is running.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Cluster ID
    expr: source.cluster_id
    comment: ID of the compute resource.
  - name: Cluster Name
    expr: cluster.cluster_name
    comment: User defined name for the cluster.
  - name: Cluster Owner
    expr: cluster.owned_by
    comment: Username of the cluster owner.
  - name: DBR Version
    expr: cluster.dbr_version
    comment: The Databricks Runtime of the cluster.
  - name: Cluster Worker Node Type
    expr: cluster.worker_node_type
    comment: Worker node type name for the cluster. Matches the cloud provider instance type.
  - name: Cluster Worker Cores
    expr: cluster.worker_core_count
    comment: Number of vCPUs for the cluster worker node type (from node_types).
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Cluster Worker Memory MB
    expr: cluster.worker_memory_mb
    comment: Total memory (MB) for the cluster worker node type (from node_types).
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Worker Pool Name
    expr: cluster.worker_pool_name
    comment: Name of the instance pool backing the cluster's workers, if any (3rd-level flake via instance_pools).
  - name: Worker Pool Cores
    expr: cluster.worker_pool_core_count
    comment: vCPUs of the worker instance pool's node type (3rd-level flake instance_pools -> node_types).
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Is Driver
    expr: source.driver
    comment: Whether the instance is a driver or worker node.
  - name: Node Type
    expr: source.node_type
    comment: The name of the node type. Matches the cloud provider instance type.

measures:
  - name: Node Minutes
    expr: COUNT(1)
    comment: Number of node-minute utilization records in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Instances
    expr: COUNT(DISTINCT source.instance_id)
    comment: Distinct number of instances in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Clusters
    expr: COUNT(DISTINCT source.cluster_id)
    comment: Distinct number of clusters in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Avg CPU User %
    expr: AVG(source.cpu_user_percent)
    comment: Average percentage of time the CPU spent in userland.
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
  - name: Avg CPU System %
    expr: AVG(source.cpu_system_percent)
    comment: Average percentage of time the CPU spent in the kernel.
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
  - name: Avg Memory Used %
    expr: AVG(source.mem_used_percent)
    comment: Average percentage of compute memory used during the period.
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
  - name: Total Network Sent Bytes
    expr: SUM(source.network_sent_bytes)
    comment: Total bytes sent out in network traffic.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Total Network Received Bytes
    expr: SUM(source.network_received_bytes)
    comment: Total bytes received from network traffic.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Total vCPU Cores
    expr: SUM(node.core_count)
    comment: Sum of vCPU cores across node-minute records (from node_types).
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Total Memory (MB)
    expr: SUM(node.memory_mb)
    comment: Sum of memory (MB) across node-minute records (from node_types).
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
