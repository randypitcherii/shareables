{#-
  Inference path B: the same UC model version behind a Model Serving
  endpoint, called from SQL with ai_query().

  dbt knows only the endpoint name (DBT_AI_ML_INFERENCE_SERVING_ENDPOINT). The
  endpoint serves one UC model version. `make ai-ml-inference-endpoint`
  creates it from serving_endpoint.json. This SQL runs on the SQL warehouse,
  and the warehouse sends the rows to the endpoint in batches.

  Use this path when the model must stay available between builds (apps,
  other pipelines, BI), or when scoring must stay in pure SQL. The endpoint
  scales to zero between builds. The first build after an idle period waits
  for a cold start.

  DISABLED until the endpoint var is set, so a clean clone still builds green.
-#}
with scored as (
    select
        input_id,
        input_text,
        ai_query('{{ var("ai_ml_inference_serving_endpoint") }}', input_text) as response
    from {{ ref('ai_ml_inference_input') }}
)

-- The endpoint returns the model's own output shape, one object per row:
-- {object, data: [{index, embedding}], usage}. Unpack it the same way the
-- Python path does, so both tables have the same columns and types.
select
    input_id,
    input_text,
    cast(response.data[0].embedding as array<float>) as embedding,
    '{{ var("ai_ml_inference_serving_endpoint") }}' as serving_endpoint
from scored
