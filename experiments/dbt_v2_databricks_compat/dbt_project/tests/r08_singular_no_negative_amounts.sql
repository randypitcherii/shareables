{{ config(tags=['r08_tests']) }}
select * from {{ ref('events') }} where amount < 0
