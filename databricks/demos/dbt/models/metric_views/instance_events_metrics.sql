{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/instance_events.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1

# Fact: system.compute.instance_events (one instance state transition).
# Inline SCD joins to instance_pools and clusters (point-in-time on event_time)
# + node_types lookup. Validated N:1 (0 many-to-many).
source: {{ source('system_compute', 'instance_events') }}

comment: Compute instance lifecycle events with point-in-time pool/cluster and node-type context.

joins:
  - name: ws
    source: {{ source('system_access', 'workspaces_latest') }}
    "on": source.workspace_id = ws.workspace_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  # FLAKE: node_types pre-joined for pool hardware. See FLAKE_JOINS.md.
  - name: pool
    source: |
      SELECT p.workspace_id, p.instance_pool_id, p.instance_pool_name, p.node_type,
             pn.core_count AS pool_core_count, pn.memory_mb AS pool_memory_mb,
             p.change_time,
             lead(p.change_time) OVER (PARTITION BY p.workspace_id, p.instance_pool_id ORDER BY p.change_time) AS next_change_time
      FROM {{ source('system_compute', 'instance_pools') }} p
      LEFT JOIN {{ source('system_compute', 'node_types') }} pn ON pn.account_id = p.account_id AND pn.node_type = p.node_type
    "on": pool.workspace_id = source.workspace_id
      AND pool.instance_pool_id = source.instance_pool_id
      AND source.event_time >= pool.change_time
      AND (source.event_time < pool.next_change_time OR pool.next_change_time IS NULL)
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
      AND cluster.cluster_id = source.cluster_id
      AND source.event_time >= cluster.change_time
      AND (source.event_time < cluster.next_change_time OR cluster.next_change_time IS NULL)
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
  - name: Event Time
    expr: source.event_time
    comment: Timestamp of the event.
  - name: Event Date
    expr: CAST(source.event_time AS DATE)
    comment: Calendar date of the event.
    format:
      type: date
      date_format: year_month_day
  - name: Workspace ID
    expr: source.workspace_id
    comment: ID of the workspace where this instance is launched.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Event Type
    expr: source.event_type
    comment: 'Event type. Possible values: INSTANCE_LAUNCHING, STATE_TRANSITION.'
  - name: State
    expr: source.state
    comment: 'Instance state. Possible values: INSTANCE_LAUNCHING, INSTANCE_READY, INSTANCE_PLACED, INSTANCE_TERMINATED.'
  - name: Availability Type
    expr: source.availability_type
    comment: 'Availability type of the instance. Possible values: ON_DEMAND, SPOT.'
  - name: Node Type
    expr: source.node_type
    comment: The name of the node type. Matches the cloud provider instance type.
  - name: Cluster Name
    expr: cluster.cluster_name
    comment: User defined name for the cluster.
  - name: Cluster Worker Node Type
    expr: cluster.worker_node_type
    comment: Worker node type name for the cluster.
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
  - name: Instance Pool Name
    expr: pool.instance_pool_name
    comment: User defined name of the instance pool.
  - name: Pool Node Cores
    expr: pool.pool_core_count
    comment: Number of vCPUs for the instance pool node type (from node_types).
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Pool Node Memory MB
    expr: pool.pool_memory_mb
    comment: Total memory (MB) for the instance pool node type (from node_types).
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

measures:
  - name: Events
    expr: COUNT(1)
    comment: Number of instance events in the slice.
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
  - name: Spot Events
    expr: COUNT(1) FILTER (WHERE source.availability_type = 'SPOT')
    comment: Number of events for SPOT-availability instances.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: On-Demand Events
    expr: COUNT(1) FILTER (WHERE source.availability_type = 'ON_DEMAND')
    comment: Number of events for ON_DEMAND-availability instances.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
