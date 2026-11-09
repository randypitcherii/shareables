{{ config(materialized='table', liquid_clustered_by=['customer_id', 'event_date'], tags=['r18_liquid_clustering']) }}
select * from {{ ref('events') }}
