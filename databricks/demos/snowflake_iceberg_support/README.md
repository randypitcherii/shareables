# Databricks-Snowflake Iceberg Integration

This project demonstrates how to configure Snowflake to read Iceberg tables from Databricks Unity Catalog using vended credentials.

## Requirements

- Databricks workspace with Unity Catalog enabled
- Access to create tables in at least one UC catalog (standard storage recommended)
- Snowflake account with privileges to create databases and catalog integrations
- Python 3.8+ environment

## Setup Instructions

### 1. Clone this repository

```bash
git clone <repository-url>
cd databricks/snowflake_iceberg_support
```

### 2. Configure Environment

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure credentials

Copy the template file and fill in your credentials:

```bash
cp .env.template .env
```

Edit the `.env` file and provide your credentials:

```
# Snowflake Credentials
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=INTERACTIVE_WH

# Databricks Unity Catalog Access
# For PyIceberg OAuth authentication using workspace-level OAuth endpoints
DATABRICKS_WORKSPACE_URL=https://your-workspace.cloud.databricks.com
DATABRICKS_SP_CLIENT_ID=your_client_id
DATABRICKS_SP_CLIENT_SECRET=your_client_secret
```

### 4. Customize configuration (optional)

If needed, edit `constants.py` to customize:
- Databricks catalog names
- Schema names
- Snowflake database name
- Snowflake catalog integration names

### 5. Run the integration

To run the full integration pipeline:

```bash
python -c "import main; main.create_tables(); main.setup_snowflake()"
```

To run just the Snowflake integration (if tables already exist in Databricks):

```bash
python -c "import main; main.test_snowflake_only()"
```

## What This Does

This integration:

1. **In Databricks:**
   - Creates Delta tables with Iceberg compatibility
   - Creates native Iceberg tables
   - Populates tables with sample data

2. **In Snowflake:**
   - Creates a database and schemas
   - Sets up catalog integrations with vended credentials
   - Creates external Iceberg tables that access Databricks tables

## Expected Results

- Successful creation of tables in Databricks
- Successful registration of standard catalog tables in Snowflake
- Tables in serverless catalog may not be accessible (expected limitation)

## Troubleshooting

- If you see PyArrow version warnings, you can install a compatible version:
  ```bash
  pip install 'pyarrow<19.0.0'
  ```

- If Snowflake can't access tables, check:
  - Service principal permissions in Databricks
  - Catalog integration configuration in Snowflake
  - Network connectivity between Snowflake and Databricks

- For "Access Denied" errors when creating Iceberg tables in serverless catalogs,
  this is an expected limitation with certain storage configurations. 