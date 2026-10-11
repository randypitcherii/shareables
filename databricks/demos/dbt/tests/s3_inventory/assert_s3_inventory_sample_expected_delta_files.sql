{{ config(enabled = s3_inventory_use_sample_data()) }}
{#-
  Correctness on the committed sample (sample mode only): every current object
  under a Delta root gets exactly the expected delta_file_status. Sample horizon
  2026-01-08 01:00 (7-day retention).
-#}
with expected (key, delta_file_status, is_kept_alive_by_other_table) as (
    values
        ('tables/sales/orders/_delta_log/00000000000000000000.json',        'delta_metadata',  false),
        ('tables/sales/orders/deletion_vector_0a1b2c.bin',                  'delta_metadata',  false),
        ('tables/sales/orders/part-00001.parquet',                          'active',          false),
        ('tables/sales/orders/part-00002.parquet',                          'active',          false),
        ('tables/sales/orders/part-00003.parquet',                          'active',          false),
        -- unreferenced since before the horizon: shouldn't be here
        ('tables/sales/orders/part-00004.parquet',                          'vacuum_eligible', false),
        -- removed inside the retention window: time travel still needs it
        ('tables/sales/orders/part-00005.parquet',                          'retained',        false),
        -- orders dropped it, the shallow clone still reads it
        ('tables/sales/orders/part-00006.parquet',                          'active',          true),
        -- written after the horizon, never committed: too new to judge
        ('tables/sales/orders/part-00007.parquet',                          'retained',        false),
        ('tables/sales/orders_clone/_delta_log/00000000000000000000.json',  'delta_metadata',  false),
        ('tables/sales/orders_clone/part-c0001.parquet',                    'active',          false),
        ('tables/sales/legacy_orders/_delta_log/00000000000000000000.json', 'delta_metadata',  false),
        ('tables/sales/legacy_orders/part-00000.parquet',                   'active',          false),
        -- s3a:// in the log, s3:// in the inventory
        ('tables/sales/orders_v2/part-00000.parquet',                       'active',          false),
        ('scratch/tmp_join/_delta_log/00000000000000000000.json',           'delta_metadata',  false),
        ('scratch/tmp_join/part-00000.parquet',                             'active',          false),
        -- tmp_join's first in-window commit removed a file that can't be named:
        -- this might be it, so it's unproven rather than vacuum_eligible
        ('scratch/tmp_join/part-00001.parquet',                             'unreferenced',    false)
),

actual as (
    select key, delta_file_status, is_kept_alive_by_other_table
    from {{ ref('int_s3_inventory__delta_file_status') }}
    where bucket = 'acme-lakehouse-prod'
)

(select 'missing or wrong' as problem, * from expected except select 'missing or wrong', * from actual)
union all
(select 'unexpected' as problem, * from actual except select 'unexpected', * from expected)
