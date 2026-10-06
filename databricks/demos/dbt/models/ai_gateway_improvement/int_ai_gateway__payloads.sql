{{ config(materialized = 'view') }}

{#-
  Every inference (payload) table this run can read, as one relation.
  See macros/ai_gateway_improvement/payload_union.sql for the discovery
  rules. Re-created on every build so newly enabled services join in.
  Payload delivery is at-least-once, so downstream dedupes on
  (request_id, invocation_id).
-#}

{{ ai_gateway_payload_union(ref('stg_ai_gateway__usage')) }}
