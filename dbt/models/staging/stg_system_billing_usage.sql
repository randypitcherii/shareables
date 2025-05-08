select
    workspace_id,
    sku_name,
    usage_quantity,
    usage_date,
    billing_origin_product,
    usage_metadata,
    custom_tags,
    identity_metadata,
    usage_unit,
    usage_start_time,
    usage_end_time
from {{ source('system_billing', 'usage') }} 