{#-
  Inference, high-resource path: the same registered model behind a Model
  Serving endpoint, called from SQL with ai_query().

  Use this when many consumers share one always-available model (apps, other
  pipelines, BI), or when scoring must stay in pure SQL on a warehouse. The
  endpoint can scale to zero, but a cold start adds minutes. The Spark-UDF path
  (audience_segment_scores_udf.py) has no endpoint to run.

  DISABLED unless `segment_serving_endpoint` (DBT_SEGMENT_SERVING_ENDPOINT)
  names the endpoint. `scripts/train_segment_classifier.py --serving-endpoint`
  creates one. The request struct field names must match the model signature,
  which is the same feature list as the training script.
-#}
{%- set features = [
    'sessions_30d', 'avg_watch_minutes', 'days_since_signup',
    'device_ctv', 'device_mobile', 'device_desktop', 'device_tablet'
] -%}

with scored as (
    select
        member_id,
        in_segment,
        ai_query(
            '{{ var("segment_serving_endpoint") }}',
            named_struct(
                {%- for f in features %}
                '{{ f }}', cast({{ f }} as double){{ "," if not loop.last }}
                {%- endfor %}
            ),
            returnType => 'ARRAY<DOUBLE>'
        ) as proba
    from {{ ref('audience_features') }}
)

select
    member_id,
    proba[1] as segment_probability,
    proba[1] >= 0.5 as predicted_in_segment,
    in_segment,
    '{{ var("segment_serving_endpoint") }}' as serving_endpoint
from scored
