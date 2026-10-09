{{ config(enabled = s3_inventory_use_sample_data()) }}
{#-
  Correctness on the committed sample (sample mode only): table attribution and
  activity. Pins the cases that break naive prefix matching:
    * orders vs orders_v2 -- a plain startswith('.../orders') would steal orders_v2
    * legacy_orders' storage_path has a trailing slash; orders_v2's uses s3a://
    * scratch/tmp_join has a _delta_log/ but no UC table -> inferred_delta_log,
      judged inactive from a PATH read
    * the VIEW and the ADLS table in the sample tables seed must be ignored
-#}
with expected (bucket, table_path, path_source, activity_status, current_bytes) as (
    values
        ('acme-lakehouse-prod', 's3://acme-lakehouse-prod/tables/sales/orders',        'unity_catalog',      'active',   354334806016),
        ('acme-lakehouse-prod', 's3://acme-lakehouse-prod/tables/sales/legacy_orders', 'unity_catalog',      'inactive', 322122549248),
        ('acme-lakehouse-prod', 's3://acme-lakehouse-prod/tables/sales/orders_v2',     'unity_catalog',      'active',    10737418240),
        ('acme-lakehouse-prod', 's3://acme-lakehouse-prod/scratch/tmp_join',           'inferred_delta_log', 'inactive',  85899346944),
        ('acme-lakehouse-prod', '(untracked)',                                         'untracked',          'n/a',       21474836480),
        ('acme-logs',           '(untracked)',                                         'untracked',          'n/a',     1743756732416),
        ('acme-archive',        '(untracked)',                                         'untracked',          'n/a',     2199023259648),
        ('acme-scratch-current-only', '(untracked)',                                   'untracked',          'n/a',     1649267441664)
),

actual as (
    select bucket, table_path, path_source, activity_status, current_bytes
    from {{ ref('s3_storage_by_table_path') }}
)

(select 'missing or wrong' as problem, * from expected except select 'missing or wrong', * from actual)
union all
(select 'unexpected' as problem, * from actual except select 'unexpected', * from expected)
