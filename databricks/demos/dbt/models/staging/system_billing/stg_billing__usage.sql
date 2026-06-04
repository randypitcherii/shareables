with source as (
    select * from {{ source('system_billing', 'usage') }}
)

select
    -- identity
    record_id,
    account_id,
    workspace_id,

    -- what was used
    sku_name,
    billing_origin_product,
    usage_type,
    cloud,

    -- when
    usage_start_time,
    usage_end_time,
    usage_date,
    ingestion_date,

    -- how much
    usage_unit,
    usage_quantity,

    -- correction handling: ORIGINAL | RETRACTION | RESTATEMENT.
    -- Summing usage_quantity across all record types lets corrections net out.
    record_type,

    -- attribution (free-form tags applied by users + compute/job tags)
    custom_tags,

    -- handy flattened compute/job identifiers from the usage_metadata struct
    usage_metadata.cluster_id    as cluster_id,
    usage_metadata.job_id        as job_id,
    usage_metadata.warehouse_id  as warehouse_id,
    usage_metadata.endpoint_name as endpoint_name,

    -- who ran it
    identity_metadata.run_as     as run_as,
    identity_metadata.created_by as created_by,

    -- product feature flags
    product_features.is_serverless as is_serverless,
    product_features.is_photon     as is_photon

from source
