# Databricks notebook source
# MAGIC %pip install pyiceberg polars pyarrow
# MAGIC %pip install --upgrade pyparsing

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import os, time

from pyiceberg.catalog import load_catalog
import polars as pl

# You'll need a tabular credential. Member credential or service account will work fine
TABULAR_CREDENTIAL       = 't-V8_i_JKUx4M:CPoAXw6dKUjFqcp4Gq0zn5iCbec'
TABULAR_TARGET_WAREHOUSE = 'enterprise_data_warehouse' # replace this with your tabular warehouse name
TABULAR_CATALOG_URI      = 'https://api.tabular.io/ws' # unless you're a single tenant user, you don't need to change this

catalog_properties = {
    'uri':        TABULAR_CATALOG_URI,
    'credential': TABULAR_CREDENTIAL,
    'warehouse':  TABULAR_TARGET_WAREHOUSE
}
catalog = load_catalog(**catalog_properties)

# COMMAND ----------

# load the weather data 🌞
tbl = catalog.load_table("batch_raw.serverless_weather_raw")
df = pl.scan_iceberg(tbl).unnest('main')

# Get the average temp by city over the last couple hours
few_hours_ago = int(time.time() - 15*60) # 2 hours ago
df_indy = pl.SQLContext(frame=df).execute(
    f"""
    select 
        name as city, 
        avg(temp) as avg_recent_temp_f,
        max(dt) as data_last_loaded_at

    from frame

    where dt > {few_hours_ago}

    group by name

    order by data_last_loaded_at desc
    """
)

df_indy_with_time = df_indy.with_columns([
    pl.col("data_last_loaded_at").cast(pl.Int64) * 1000000
]).with_columns([
    pl.col(
        "data_last_loaded_at"
    ).cast(
        pl.Datetime
    ).dt.convert_time_zone(
        'America/New_York'
    ).dt.strftime("%Y-%m-%d %I:%M:%S")
])


print('Polars 🐻‍❄️ average temp by city over the last few hours:')
df_indy_with_time.collect().glimpse

# COMMAND ----------

# load the weather data 🌞
tbl = catalog.load_table("batch_raw.serverless_weather_raw_deduplicated")
df = pl.scan_iceberg(tbl).unnest('main')

# Get the average temp by city over the last couple hours
few_hours_ago = int(time.time() - 48*60*60) # 24 hours ago
df_indy = pl.SQLContext(frame=df).execute(
    f"""
    select 
        name as city, 
        avg(temp) as avg_recent_temp_f,
        max(dt) as data_last_loaded_at

    from frame

    where dt > {few_hours_ago}

    group by name

    order by data_last_loaded_at desc
    """
)

df_indy_with_time = df_indy.with_columns([
    pl.col("data_last_loaded_at").cast(pl.Int64) * 1000000
]).with_columns([
    pl.col(
        "data_last_loaded_at"
    ).cast(
        pl.Datetime
    ).dt.convert_time_zone(
        'America/New_York'
    ).dt.strftime("%Y-%m-%d %I:%M:%S")
])


print('Polars 🐻‍❄️ average temp by city over the last few hours:')
df_indy_with_time.collect().glimpse

# COMMAND ----------

from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform

# get table
table = catalog.load_table("batch_raw.serverless_weather_raw_deduplicated")

# create a sort order
sort_order = SortOrder(SortField(source_id=1, transform=IdentityTransform(), direction="desc"))

property = {"write.sort-order": str(sort_order)}

with table.transaction() as transaction:
    transaction.set_properties(**property)

# COMMAND ----------

# investigate how a table with an existing sort order appears
weather_table = catalog.load_table("batch_raw.serverless_weather_raw")
weather_table.properties

# COMMAND ----------

my_var = 1 / 0
