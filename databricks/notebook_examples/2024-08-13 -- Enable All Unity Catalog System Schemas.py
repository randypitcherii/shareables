# Databricks notebook source
# MAGIC %md
# MAGIC # Enable ALL UnityCatalog System Schemas
# MAGIC 👋 Hey! 
# MAGIC
# MAGIC Stuck like I was trying to use cool system tables in databricks like the queryable Query History table?
# MAGIC
# MAGIC It's a pain to enable these as you need to call a databricks API. You know what they say - the one thing SQL users love is writing API requests 💀 
# MAGIC
# MAGIC Use this notebook to enable all of them and move on with your cool life 😎
# MAGIC
# MAGIC **NOTE:** No worries if you already have some of these enabled. The code will just skip schemas that are already turned on 🧠
# MAGIC
# MAGIC ## Auth setup
# MAGIC 1. Get yourself a PAT. Click your user icon in the top right of this page > settings > developer (NOT development) > access tokens > create a new one > copy it and come back here
# MAGIC 2. you can paste it directly below if you're lazy. 
# MAGIC `PAT_TOKEN = 'my_pat'`
# MAGIC 3. if you're trying to do this by the book though, you should store this as a databricks secret.
# MAGIC - to do this, install and configure the databricks CLI (it is insane to me there is no UI for adding secrets 🤯)
# MAGIC - run `databricks secrets create-scope randy_pitcher_workspace` to create your secret scope
# MAGIC - run `databricks secrets put-secret randy_pitcher_workspace databricks_pat --string-value your_pat_here` to save the secret
# MAGIC - update the `PAT_TOKEN` variable below to access your specific scope and key 💪
# MAGIC
# MAGIC
# MAGIC 🚀 With that out of the way, feel free to run the whole workbook! Serverless notebook compute works great for this. 🔥

# COMMAND ----------

import requests

# 🔥 Replace with your actual value 💽
PAT_TOKEN = dbutils.secrets.get(scope="randy_pitcher_workspace", key="databricks_pat")

def enable_system_schemas():
    # get workspace and metastore ids
    base_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
    metastore_id = spark.sql("select split(current_metastore(), ':')[2] as metastore_id").collect()[0]['metastore_id']
    
    # Headers for authentication
    headers = {
        'Authorization': f'Bearer {PAT_TOKEN}'
    }

    # Base URL for API requests
    base_url = f'https://{base_url}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas'

    # Get the list of available system schemas
    response = requests.get(base_url, headers=headers)
    schemas = response.json().get('schemas', [])

    # Iterate through schemas and enable them if not already enabled
    for schema in schemas:
        schema_name = schema['schema']
        state = schema['state']
        
        print(f'🔎 found schema {schema_name} with state={state}')
        if state == 'AVAILABLE':
            enable_url = f'{base_url}/{schema_name}'
            enable_response = requests.put(enable_url, headers=headers)
            if enable_response.status_code == 200:
                print(f'\t✅ Successfully enabled schema: {schema_name}')
            else:
                print(f'\t❌ Failed to enable schema: {schema_name}, Error: {enable_response.json()}')
        else:
            print(f'\t🆗 Schema {schema_name} is already enabled.')
        
        print('\n\n') # spacer for log output between iterations


# COMMAND ----------

enable_system_schemas()

