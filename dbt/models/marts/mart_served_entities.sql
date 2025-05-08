-- This mart model directly reflects the 'latest_model_serving_endpoints' CTE identified from the dashboard.
-- It was a separate dataset in the dashboard with displayName 'served_entities'.
select 
    endpoint_id,
    endpoint_name,
    endpoint_created_by,
    workspace_id,
    last_updated_at,
    is_deleted
from {{ ref('dim_latest_model_serving_endpoints') }} 