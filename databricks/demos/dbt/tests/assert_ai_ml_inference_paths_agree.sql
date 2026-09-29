{{ config(
    tags=['ai_ml_inference'],
    enabled=var('ai_ml_inference_serving_endpoint', '') != ''
) }}
{#-
  Both inference paths apply the SAME UC model version to the SAME rows, so
  they must agree. The endpoint's container and the Python model's pinned
  environment can use different library versions, so this test compares
  cosine similarity instead of exact equality. A real difference, such as a
  different model version, the wrong input column, or rows out of order,
  gives a similarity far below this threshold.
-#}
with paired as (
    select
        python.input_id,
        python.embedding as a,
        serving.embedding as b
    from {{ ref('ai_ml_inference_embeddings_python') }} as python
    full outer join {{ ref('ai_ml_inference_embeddings_serving') }} as serving
        on python.input_id = serving.input_id
),

scored as (
    select
        input_id,
        aggregate(zip_with(a, b, (x, y) -> x * y), 0D, (acc, v) -> acc + v)
            / (sqrt(aggregate(a, 0D, (acc, v) -> acc + v * v))
               * sqrt(aggregate(b, 0D, (acc, v) -> acc + v * v))) as cosine_similarity
    from paired
)

select *
from scored
where cosine_similarity is null  -- a row missing from one side
   or cosine_similarity < 0.999
