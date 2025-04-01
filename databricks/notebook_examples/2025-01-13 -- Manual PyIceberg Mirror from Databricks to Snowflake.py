{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "dae335c3-7c70-44ef-a631-f82b34c54f69",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# Snowflake Setup\n",
    "It's time to set up some access in Snowflake. Here's what you'll need:\n",
    "- a user account for this code to use to issue iceberg table create commands\n",
    "- that user account needs to have a default role assigned with access to:\n",
    "  - create tables in your desired database/schema \n",
    "  - use a data warehouse\n",
    "  - use the catalog and external volume you'll create below\n",
    "\n",
    "That's it! Probably!\n",
    "\n",
    "Here's a sample Snowflake script to get this cooking. The emojis point out places where you need to add values yourself:\n",
    "```sql\n",
    "// -----------------------------------------------\n",
    "// create security admin assets\n",
    "// -----------------------------------------------\n",
    "use role securityadmin;\n",
    "create role pyiceberg_mirroring_service_role;\n",
    "grant role pyiceberg_mirroring_service_role to role sysadmin; \n",
    "\n",
    "// you can create these if they do not exist. This guide is just a starting point\n",
    "grant usage on database my_target_database🪐 to role pyiceberg_mirroring_service_role;\n",
    "grant ownership on schema my_target_database🪐.my_mirroring_schema🧊 to role pyiceberg_mirroring_service_role copy grants;\n",
    "grant usage on warehouse some_cool_warehouse 🪐 to role pyiceberg_mirroring_service_role; // an xs is plenty here. Just doing some metadata ops 💪\n",
    "\n",
    "create user pyiceberg_mirroring_service_user \n",
    "  password = '😎',\n",
    "  must_change_password = false\n",
    "  default_role = pyiceberg_mirroring_service_role,\n",
    "  default_warehouse = some_cool_warehouse 🪐; \n",
    "  \n",
    "grant role pyiceberg_mirroring_service_role to user pyiceberg_mirroring_service_user;\n",
    "// -----------------------------------------------\n",
    "\n",
    "\n",
    "// -----------------------------------------------\n",
    "// create account admin assets \n",
    "// -----------------------------------------------\n",
    "use role accountadmin;\n",
    "create external volume databricks_unity_catalog_volume // I like to name these same as my warehouse, so enterprise_data_warehouse_volume for example \n",
    "  allow_writes=false\n",
    "  storage_locations = ((\n",
    "      name = 🌞'databricks_unity_catalog_volume'\n",
    "      storage_provider = 'S3'\n",
    "      storage_aws_role_arn = 🌞'arn:aws:iam::account_id:role/snowflake-databricks-my-cool-warehouse-volume' // call this what you like. We will create the role in your aws account after this\n",
    "      storage_base_url = 🌞's3://my-s3-bucket-where-unity-catalog-puts-my-data/' // this is the s3 location of your tabular warehouse.\n",
    "  ));\n",
    "\n",
    "create catalog integration databricks_unity_catalog_my_cool_catalog\n",
    "  catalog_source = object_store\n",
    "  table_format = iceberg\n",
    "  enabled = true\n",
    "  comment = 'Catalog Integration for reading Databricks Unity Catalog Iceberg tables';\n",
    "  \n",
    "// let the service role use these nifty new iceberg objects\n",
    "grant usage on integration databricks_unity_catalog_my_cool_catalog to role pyiceberg_mirroring_service_role;\n",
    "grant usage on volume databricks_unity_catalog_volume to role pyiceberg_mirroring_service_role;\n",
    "// -----------------------------------------------\n",
    "\n",
    "\n",
    "// -----------------------------------------------\n",
    "// Get details from newly-created integration \n",
    "// objects. \n",
    "// -----------------------------------------------\n",
    "use role pyiceberg_mirroring_service_role;\n",
    "use warehouse some_cool_warehouse 🪐;\n",
    "describe external volume databricks_unity_catalog_volume;\n",
    "\n",
    "\n",
    "// this query will give you 2 values you will need to create the snowflake access role in AWS\n",
    "select \n",
    "  parse_json(\"property_value\"::string):\"STORAGE_AWS_ROLE_ARN\"::string as storage_aws_iam_role_arn,\n",
    "  parse_json(\"property_value\"::string):\"STORAGE_AWS_IAM_USER_ARN\"::string as storage_aws_iam_user_arn,\n",
    "  parse_json(\"property_value\"::string):\"STORAGE_AWS_EXTERNAL_ID\"::string as storage_aws_external_id\n",
    "  \n",
    "from table(result_scan(last_query_id()))\n",
    "\n",
    "where \"parent_property\"='STORAGE_LOCATIONS' and \"property\"='STORAGE_LOCATION_1';\n",
    "\n",
    "```\n",
    "\n",
    "🚧 Quick check!\n",
    "- make sure to grab those three important fields from Snowflake -- or just keep them handy for the AWS step\n",
    "- I attached a screenshot of my results. You should see different values but the same kinda thing after running the final SQL statement in the script above.\n",
    "\n",
    "![image.png](attachment:image.png)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "75da48b8-a370-414f-9034-7467a315b4b2",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 3. AWS Setup\n",
    "No one panic, but it's time to log in to AWS and go to IAM and create a new policy + role 💪\n",
    "\n",
    "### Create Policy\n",
    "- let's start with the policy. We need to build a read only policy for your S3 location that holds the Iceberg warehouse you want to mirror\n",
    "\n",
    "```json\n",
    "{\n",
    "    \"Version\": \"2012-10-17\",\n",
    "    \"Statement\": [\n",
    "        {\n",
    "            \"Action\": [\n",
    "                \"s3:GetObject\",\n",
    "                \"s3:GetObjectVersion\"\n",
    "            ],\n",
    "            \"Resource\": [\n",
    "                \"arn:aws:s3:::your_s3_bucket🦆/*\"\n",
    "            ],\n",
    "            \"Effect\": \"Allow\"\n",
    "        },\n",
    "        {\n",
    "            \"Action\": [\n",
    "                \"s3:ListBucket\",\n",
    "                \"s3:GetBucketLocation\"\n",
    "            ],\n",
    "            \"Resource\": [\n",
    "                \"arn:aws:s3:::your_s3_bucket🦆\"\n",
    "            ],\n",
    "            \"Effect\": \"Allow\"\n",
    "        }\n",
    "    ]\n",
    "}\n",
    "```\n",
    "\n",
    "### Create Role, *with Trust Policy*\n",
    "- AWS > IAM > Create Role\n",
    "- Important! You MUST name this role identically to what you told Snowflake to look for. So make sure those values are the same. This will be the `STORAGE_AWS_IAM_ROLE_ARN` value you received in your final Snowflake SQL query in the previous Snowflake Setup step 💪\n",
    "- we need to attach the IAM Policy that you created above to this role\n",
    "- now for the tricky part -- let's add a trust relationship so snowflake can use this role\n",
    "  - remember the Snowflake values from the snowflake step? We need those. You should have\n",
    "      - Snowflake AWS User ARN\n",
    "      - Snowflake AWS External ID\n",
    "  - these are super important, make sure to copy them EXACTLY! I mean it!\n",
    "\n",
    "Lastly, here is a sample of the trust policy you'll need.\n",
    "\n",
    "```json\n",
    "{\n",
    "    \"Version\": \"2008-10-17\",\n",
    "    \"Statement\": [\n",
    "        {\n",
    "            \"Effect\": \"Allow\",\n",
    "            \"Principal\": {\n",
    "                \"AWS\": \"snowflake user arn here ❄️\"\n",
    "            },\n",
    "            \"Action\": \"sts:AssumeRole\",\n",
    "            \"Condition\": {\n",
    "                \"StringEquals\": {\n",
    "                    \"sts:ExternalId\": \"🏂 snowflake external ID here\"\n",
    "                }\n",
    "            }\n",
    "        }\n",
    "    ]\n",
    "}\n",
    "```\n",
    "\n",
    "💾 Suplex that save button and let's get down to business!\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "76cedb34-57a9-487d-b61d-0ccb363a273c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 4. Pyiceberg Mirroring (aka, getting down to business 🕴️)\n",
    "- remember everything we've been through. We're bonded after all this effort above\n",
    "- keep the following values handy. I recommend putting a few of them in your `.env` file\n",
    "  - you'll need your snowflake account id. You could stumble through the docs for this, OR just copy the results of running this in snowflake. Save this in `.env` as `SNOWFLAKE_ACCOUNT_IDENTIFIER`.\n",
    "  ```sql\n",
    "  select current_organization_name() || '-' || current_account_name() as snowflake_account_identifier;\n",
    "  ```\n",
    "  - you'll need your snowflake service user login name and password.\n",
    "    - set these in your .env as `SNOWFLAKE_USERNAME` and `SNOWFLAKE_PASSWORD`\n",
    "  - you tabular credential should already be tucked safe and sound in your .env file as `TABULAR_CREDENTIAL`\n",
    "\n",
    "Now we're ready to cook 🍳\n",
    "\n",
    "### ⚠️ Double check:\n",
    "- Seriously, make sure you save that .env file. \n",
    "- if this is scary, you can ignore the `.env` file and just paste your credential in plaintext directly in this notebook -- but you should feel bad about your craftsmanship.\n",
    "\n",
    "\n",
    "*One last note* -- you definitely don't have the same data I do. Make sure you use your own configs as required, but this should be a good starting point for you."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "23401591-950c-4dbf-bbd8-0f3bdf7a6f7c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "%pip install pyiceberg[pyarrow]\n",
    "%pip install --upgrade pyparsing\n",
    "%restart_python"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "98040001-4c6d-4332-b74a-66a89e590a89",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from pyiceberg.catalog import load_catalog\n",
    "from pyiceberg.exceptions import TableAlreadyExistsError\n",
    "\n",
    "# Databricks config\n",
    "UC_CATALOG_TO_MIRROR = 'analytics_prod' # replace with the catalog name you want to mirror.\n",
    "UC_CREDENTIAL  = dbutils.secrets.get(scope=\"randy_pitcher_workspace\", key=\"databricks_pat\")\n",
    "UC_DATABRICKS_URL = f'{dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()}'\n",
    "UC_CATALOG_URI = f'https://{UC_DATABRICKS_URL}/api/2.1/unity-catalog/iceberg'\n",
    "\n",
    "# Snowflake config\n",
    "SNOWFLAKE_VOLUME  = 'databricks_uc_volume' # You created this in your Snowflake script. Please copy/paste that value here\n",
    "SNOWFLAKE_CATALOG = 'databricks_uc_catalog' # You created this in your Snowflake script. Please copy/paste that value here\n",
    "SNOWFLAKE_USERNAME           = dbutils.secrets.get(scope=\"randy_pitcher_workspace\", key=\"SNOWFLAKE_USERNAME\")\n",
    "SNOWFLAKE_PASSWORD           = dbutils.secrets.get(scope=\"randy_pitcher_workspace\", key=\"SNOWFLAKE_PASSWORD\")\n",
    "SNOWFLAKE_ACCOUNT_IDENTIFIER = dbutils.secrets.get(scope=\"randy_pitcher_workspace\", key=\"SNOWFLAKE_ACCOUNT_IDENTIFIER\")\n",
    "SNOWFLAKE_DATABASE           = 'databricks_unity_catalog' # this should already exist in snowflake\n",
    "\n",
    "\n",
    "catalog_properties = {\n",
    "    'type':      'rest',\n",
    "    'uri':       UC_CATALOG_URI,\n",
    "    'token':     UC_CREDENTIAL,\n",
    "    'warehouse': UC_CATALOG_TO_MIRROR\n",
    "}\n",
    "catalog = load_catalog(**catalog_properties)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "e87cebbd-0c36-4001-9c0f-232fcd2fd84c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# get tables to build mirrors for 💪\n",
    "tables_to_mirror = []\n",
    "namespaces_to_register = [namespace[0] for namespace in catalog.list_namespaces() if namespace[0] not in ['information_schema']]\n",
    "for namespace in namespaces_to_register: \n",
    "  print(f'\\n\\nchecking {UC_CATALOG_TO_MIRROR}.{namespace}:')\n",
    "  for _, tablename in catalog.list_tables(namespace):\n",
    "    tables_to_mirror.append(catalog.load_table(f\"{namespace}.{tablename}\"))\n",
    "    print(f\"\\tFound iceberg table: '{UC_CATALOG_TO_MIRROR}.{namespace}.{tables_to_mirror[-1].identifier[-1]}'\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "d200184e-a2ab-4672-a091-b1c0c5610361",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import snowflake.connector\n",
    "\n",
    "# Snowflake connection parameters\n",
    "# These variables are gathered in the initial python cell above ⬆️\n",
    "snowflake_config = {\n",
    "    \"user\": SNOWFLAKE_USERNAME,\n",
    "    \"password\": SNOWFLAKE_PASSWORD,\n",
    "    \"account\": SNOWFLAKE_ACCOUNT_IDENTIFIER\n",
    "}\n",
    "\n",
    "# Create a connection object\n",
    "snowflake_conn = snowflake.connector.connect(**snowflake_config)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "d9ddc7e3-1548-4342-b16b-701cc4e2e76d",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Validate the connection details before mirroring\n",
    "try:\n",
    "  curs = snowflake_conn.cursor()\n",
    "\n",
    "  # set session configs\n",
    "  curs.execute(f'use database {SNOWFLAKE_DATABASE}')\n",
    "\n",
    "  # validation query\n",
    "  curs.execute('select current_user(), current_role(), current_database(), current_warehouse(), 1=1 as warehouse_is_usable')\n",
    "  row = curs.fetchone()\n",
    "  print(f\"\"\"\n",
    "    Snowflake connection validity check:\n",
    "      - Current User:         '{row[0]}'\n",
    "      - Current Role:         '{row[1]}'\n",
    "      - Current Database:     '{row[2]}'\n",
    "      - Current Warehouse:    '{row[3]}'\n",
    "      - Warehouse is usable?: '{row[4]}'\"\"\")\n",
    "  \n",
    "except Exception as e:\n",
    "  print(f'Snowflake connection error:\\n{e}')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "95907aa3-37d7-4afc-838f-00ed492ce4c8",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Build snowflake mirrors 💪\n",
    "# test query to validate the basics\n",
    "curs = snowflake_conn.cursor()\n",
    "for table_to_mirror in tables_to_mirror:\n",
    "  try:    \n",
    "    # grab the table name to use.\n",
    "    # Note: If you expect naming collisions, you can prefix these by their schema name or whatever you like\n",
    "    snowflake_mirror_schema = table_to_mirror.identifier[-2] # second to last part = schema\n",
    "    snowflake_mirror_tablename = table_to_mirror.identifier[-1] # last part of the identifier is the table name\n",
    "    metadata_file_path = '/'.join(table_to_mirror.metadata_location.split('/')[-3:])\n",
    "\n",
    "    # create destination schema if it doesn't exist\n",
    "    curs.execute(f'create schema if not exists {SNOWFLAKE_DATABASE}.{snowflake_mirror_schema}')\n",
    "\n",
    "    # mirror the table\n",
    "    mirror_query = f\"\"\"\n",
    "      create or replace iceberg table {SNOWFLAKE_DATABASE}.{snowflake_mirror_schema}.{snowflake_mirror_tablename}\n",
    "        external_volume = '{SNOWFLAKE_VOLUME}'\n",
    "        catalog = '{SNOWFLAKE_CATALOG}'\n",
    "        metadata_file_path = '{metadata_file_path}'\n",
    "        copy grants;\"\"\"\n",
    "    print(f\"Attempting mirror command:{mirror_query}\")\n",
    "    curs.execute(mirror_query)\n",
    "    print(f\"✅ Success!\\n\\n\")\n",
    "    \n",
    "  except Exception as e:\n",
    "    print(f'❌ Failure. Snowflake mirroring error for table \"{table_to_mirror.identifier[1]}.{table_to_mirror.identifier[2]}\":\\n{e}')\n",
    "    "
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 2
   },
   "notebookName": "pyiceberg_mirror_to_snowflake",
   "widgets": {}
  },
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
