# Databricks notebook source
# MAGIC %pip install pyiceberg[arrow,adlfs,sql-sqlite]
# MAGIC %restart_python

# COMMAND ----------

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    NestedField,
)
import os

# Azure Blob Storage details
storage_account = "oneenvstorage"
container = "randyworkspaceuseastblob"
sas_token = "shhh" # replace this please

# define local catalog
warehouse_path = './iceberg_warehouse'
os.makedirs(warehouse_path, exist_ok=True)  # Ensure the directory exists

catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": f"file://{warehouse_path}",
        "adlfs.account-name": storage_account,
        "adlfs.sas-token": sas_token
    },
)

# COMMAND ----------

schema = Schema(
  NestedField(field_id=1, name="greeting", field_type=StringType(), required=False),
)

catalog.create_table(
    identifier="randy.howdy",
    schema=schema,
location=f'abfs://{container}@{storage_account}.dfs.core.windows.net/iceberg/hello'
)
