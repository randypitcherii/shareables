{{ config(materialized='table', auto_liquid_cluster=true, tags=['r18_liquid_clustering']) }}
select * from {{ ref('events') }}
