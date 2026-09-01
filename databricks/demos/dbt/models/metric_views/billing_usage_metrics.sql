{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/billing_usage.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1

# Fact: system.billing.usage (one billable usage record).
# SCD dimensions are defined INLINE as join sources (raw dim + computed
# next_change_time via lead()), so no helper views are needed -- the metric
# view is fully self-contained. Point-in-time range predicates live in each
# join `on`. Product-scoped edges (cluster/job/pipeline) carry the
# billing_origin_product predicate inside `on` so the FK only attaches where
# valid. All joins validated N:1 (0 many-to-many).
source: {{ source('system_billing', 'usage') }}

comment: Databricks billable usage (cost/DBU) at usage-record grain, enriched with
  point-in-time workspace, price, compute, job, pipeline and serving-endpoint context.

joins:
  # workspace: snapshot equi-join
  - name: ws
    source: {{ source('system_access', 'workspaces_latest') }}
    "on": source.workspace_id = ws.workspace_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  # list price: account-scoped, point-in-time on the SKU price window
  - name: price
    source: {{ source('system_billing', 'list_prices') }}
    "on": price.sku_name = source.sku_name
      AND source.usage_start_time >= price.price_start_time
      AND (source.usage_start_time < price.price_end_time OR price.price_end_time IS NULL)
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  # cluster: inline SCD, point-in-time + scoped to classic compute products.
  # FLAKE: node_types pre-joined inside the source (worker + driver hardware) so
  # the 2nd-level attributes ride along as parent-dim columns. See FLAKE_JOINS.md.
  - name: cluster
    source: |
      SELECT c.workspace_id, c.cluster_id, c.cluster_name, c.owned_by, c.dbr_version,
             c.data_security_mode,
             wn.core_count AS worker_core_count, wn.memory_mb AS worker_memory_mb, wn.gpu_count AS worker_gpu_count,
             dn.core_count AS driver_core_count, dn.memory_mb AS driver_memory_mb,
             ip.instance_pool_name AS worker_pool_name, ipn.core_count AS worker_pool_core_count,
             c.change_time,
             lead(c.change_time) OVER (PARTITION BY c.workspace_id, c.cluster_id ORDER BY c.change_time) AS next_change_time
      FROM {{ source('system_compute', 'clusters') }} c
      LEFT JOIN {{ source('system_compute', 'node_types') }} wn ON wn.account_id = c.account_id AND wn.node_type = c.worker_node_type
      LEFT JOIN {{ source('system_compute', 'node_types') }} dn ON dn.account_id = c.account_id AND dn.node_type = c.driver_node_type
      LEFT JOIN (
        SELECT workspace_id, instance_pool_id, instance_pool_name, node_type, change_time,
               lead(change_time) OVER (PARTITION BY workspace_id, instance_pool_id ORDER BY change_time) AS next_change_time
        FROM {{ source('system_compute', 'instance_pools') }}
      ) ip ON ip.workspace_id = c.workspace_id AND ip.instance_pool_id = c.worker_instance_pool_id
        AND c.change_time >= ip.change_time AND (c.change_time < ip.next_change_time OR ip.next_change_time IS NULL)
      LEFT JOIN {{ source('system_compute', 'node_types') }} ipn ON ipn.account_id = c.account_id AND ipn.node_type = ip.node_type
    "on": cluster.workspace_id = source.workspace_id
      AND cluster.cluster_id = source.usage_metadata.cluster_id
      AND source.billing_origin_product IN ('JOBS', 'ALL_PURPOSE', 'DLT')
      AND source.usage_start_time >= cluster.change_time
      AND (source.usage_start_time < cluster.next_change_time OR cluster.next_change_time IS NULL)
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  # SQL warehouse: inline SCD, point-in-time
  - name: warehouse
    source: |
      SELECT workspace_id, warehouse_id, warehouse_name, warehouse_type, warehouse_size, change_time,
             lead(change_time) OVER (PARTITION BY workspace_id, warehouse_id ORDER BY change_time) AS next_change_time
      FROM {{ source('system_compute', 'warehouses') }}
    "on": warehouse.workspace_id = source.workspace_id
      AND warehouse.warehouse_id = source.usage_metadata.warehouse_id
      AND source.usage_start_time >= warehouse.change_time
      AND (source.usage_start_time < warehouse.next_change_time OR warehouse.next_change_time IS NULL)
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  # instance pool: inline SCD, point-in-time. FLAKE: node_types pre-joined for pool hardware.
  - name: pool
    source: |
      SELECT p.workspace_id, p.instance_pool_id, p.instance_pool_name, p.node_type,
             pn.core_count AS pool_core_count, pn.memory_mb AS pool_memory_mb,
             p.change_time,
             lead(p.change_time) OVER (PARTITION BY p.workspace_id, p.instance_pool_id ORDER BY p.change_time) AS next_change_time
      FROM {{ source('system_compute', 'instance_pools') }} p
      LEFT JOIN {{ source('system_compute', 'node_types') }} pn ON pn.account_id = p.account_id AND pn.node_type = p.node_type
    "on": pool.workspace_id = source.workspace_id
      AND pool.instance_pool_id = source.usage_metadata.instance_pool_id
      AND source.usage_start_time >= pool.change_time
      AND (source.usage_start_time < pool.next_change_time OR pool.next_change_time IS NULL)
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  # job: inline SCD, point-in-time + scoped to JOBS product
  - name: job
    source: |
      SELECT workspace_id, job_id, name AS job_name, creator_id, run_as, change_time,
             lead(change_time) OVER (PARTITION BY workspace_id, job_id ORDER BY change_time) AS next_change_time
      FROM {{ source('system_lakeflow', 'jobs') }}
    "on": job.workspace_id = source.workspace_id
      AND job.job_id = source.usage_metadata.job_id
      AND source.billing_origin_product = 'JOBS'
      AND source.usage_start_time >= job.change_time
      AND (source.usage_start_time < job.next_change_time OR job.next_change_time IS NULL)
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  # pipeline: inline SCD, point-in-time + scoped to DLT product
  - name: pipeline
    source: |
      SELECT workspace_id, pipeline_id, name AS pipeline_name, pipeline_type, created_by, run_as, change_time,
             lead(change_time) OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time) AS next_change_time
      FROM {{ source('system_lakeflow', 'pipelines') }}
    "on": pipeline.workspace_id = source.workspace_id
      AND pipeline.pipeline_id = source.usage_metadata.dlt_pipeline_id
      AND source.billing_origin_product = 'DLT'
      AND source.usage_start_time >= pipeline.change_time
      AND (source.usage_start_time < pipeline.next_change_time OR pipeline.next_change_time IS NULL)
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  # serving endpoint / entity: inline SCD, point-in-time
  - name: served
    source: |
      SELECT workspace_id, served_entity_id, endpoint_name, served_entity_name, entity_type,
             entity_name, entity_version, change_time,
             lead(change_time) OVER (PARTITION BY workspace_id, served_entity_id ORDER BY change_time) AS next_change_time
      FROM {{ source('system_serving', 'served_entities') }}
    "on": served.workspace_id = source.workspace_id
      AND served.served_entity_id = source.usage_metadata.endpoint_id
      AND source.usage_start_time >= served.change_time
      AND (source.usage_start_time < served.next_change_time OR served.next_change_time IS NULL)
    cardinality: many_to_one
    rely:
      at_most_one_match: true

dimensions:
  - name: Usage Date
    expr: source.usage_date
    comment: Date of the usage record, this field can be used for faster aggregation by date.
    format:
      type: date
      date_format: year_month_day
  - name: Usage Month
    expr: DATE_TRUNC('MONTH', source.usage_date)
    comment: Month bucket derived from the usage date for monthly trend analysis.
    format:
      type: date
      date_format: locale_short_month
  - name: Workspace ID
    expr: source.workspace_id
    comment: ID of the Workspace this usage was associated with.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Account ID
    expr: source.account_id
    comment: ID of the account this report was generated for.
  - name: SKU
    expr: source.sku_name
    comment: Name of the SKU.
  - name: Cloud
    expr: source.cloud
    comment: Cloud this usage is relevant for. Possible values are AWS, AZURE, and GCP.
  - name: Billing Origin Product
    expr: source.billing_origin_product
    comment: The product that originated the usage. Some products can be billed as different SKUs.
  - name: Usage Unit
    expr: source.usage_unit
    comment: Unit this usage is measured in. Possible values include DBUs.
  - name: Cluster Name
    expr: cluster.cluster_name
    comment: User defined name for the cluster.
  - name: Cluster Owner
    expr: cluster.owned_by
    comment: Username of the cluster owner. Defaults to the cluster creator.
  - name: DBR Version
    expr: cluster.dbr_version
    comment: The Databricks Runtime of the cluster.
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
  - name: Cluster Worker GPUs
    expr: cluster.worker_gpu_count
    comment: Number of GPUs for the cluster worker node type (from node_types).
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Cluster Driver Cores
    expr: cluster.driver_core_count
    comment: Number of vCPUs for the cluster driver node type (from node_types).
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
  - name: Warehouse Name
    expr: warehouse.warehouse_name
    comment: The name of the SQL warehouse.
  - name: Warehouse Size
    expr: warehouse.warehouse_size
    comment: The cluster size of the SQL warehouse (e.g. SMALL, MEDIUM, LARGE).
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
  - name: Job Name
    expr: job.job_name
    comment: The user-supplied name of the job.
  - name: Pipeline Name
    expr: pipeline.pipeline_name
    comment: The user-supplied name of the pipeline.
  - name: Serving Endpoint
    expr: served.endpoint_name
    comment: The name of the serving endpoint.
  - name: Served Entity
    expr: served.served_entity_name
    comment: The name of the served entity.

measures:
  - name: Usage Records
    expr: COUNT(1)
    comment: Number of usage records in the selected slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Usage Quantity
    expr: SUM(source.usage_quantity)
    comment: Total number of units consumed (e.g. DBUs).
    format:
      type: number
      decimal_places:
        type: exact
        places: 2
  - name: List Cost (USD)
    expr: SUM(source.usage_quantity * COALESCE(price.pricing.effective_list.default, price.pricing.default))
    comment: Estimated list-price cost = usage_quantity x effective list price.
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
  - name: Distinct Workspaces
    expr: COUNT(DISTINCT source.workspace_id)
    comment: Distinct number of workspaces with activity in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct SKUs
    expr: COUNT(DISTINCT source.sku_name)
    comment: Distinct number of SKUs in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
