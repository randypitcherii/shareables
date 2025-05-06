# dbt Project Setup

This guide provides instructions for setting up your [dbt](https://docs.getdbt.com/) environment to work with this project, focusing on connecting to Databricks.

## 1. Python Environment and dbt Installation

You'll need Python installed. Then, you can use either `uv` (preferred) or `poetry` to manage your dbt installation and dependencies.

### Using `uv` (Preferred)

`uv` is a fast Python package installer and resolver.

This project uses `uv` and includes a `pyproject.toml` and `uv.lock` file in the `dbt/` directory to manage Python dependencies. These files should be committed to version control.

1.  **Install `uv`**:
    If you don't have `uv` installed, follow the official installation instructions: [https://github.com/astral-sh/uv#installation](https://github.com/astral-sh/uv#installation)

2.  **Navigate to the `dbt/` directory**:
    In your terminal:
    ```bash
    cd path/to/your/shareables/dbt
    ```

3.  **Create/Update Virtual Environment and Install Dependencies**:
    Run the following command. `uv sync` will use the existing `pyproject.toml` and `uv.lock` files to create a virtual environment (if one named `.venv` doesn't already exist) and install the exact locked versions of the dependencies.
    ```bash
    uv sync
    ```

4.  **Activate the Virtual Environment**:
    ```bash
    source .venv/bin/activate  # On macOS/Linux
    # .venv\Scripts\activate   # On Windows
    ```
    After activation, your prompt should change, and you can proceed to use `dbt` commands.

### Using `poetry`

Poetry is another popular tool for dependency management in Python.

1.  **Install `poetry`**:
    If you don't have Poetry, refer to the official Poetry installation guide: [https://python-poetry.org/docs/#installation](https://python-poetry.org/docs/#installation)

2.  **Initialize Poetry and Add Dependencies (if starting fresh in this directory)**:
    If there isn't a `pyproject.toml` file in the `dbt/` directory configured for this dbt project, you might want to initialize Poetry.
    ```bash
    cd path/to/your/shareables/dbt
    poetry init  # Follow the prompts
    ```
    Then add the necessary packages:
    ```bash
    poetry add dbt-core dbt-databricks
    ```
    This will add the latest versions to your `pyproject.toml` and install them.

3.  **Activate Poetry Shell**:
    To activate the environment managed by Poetry:
    ```bash
    poetry shell
    ```

## 2. Configure Your dbt Profile for Databricks

dbt needs a `profiles.yml` file to know how to connect to your data warehouse (Databricks, in this case). This file typically resides in `~/.dbt/` (your user's home dbt directory), but can also be placed in your project directory (though be careful with credentials).

*   **Official Documentation**: For detailed information on `profiles.yml`, refer to the dbt documentation: [https://docs.getdbt.com/docs/core/connect-data-platform/profiles.yml](https://docs.getdbt.com/docs/core/connect-data-platform/profiles.yml)
*   **Databricks Specific Setup**: Also see the Databricks setup page: [https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)

### Sample `profiles.yml` for Databricks:

Create or update your `~/.dbt/profiles.yml` file. Give your profile a name (e.g., `my_databricks_project`) and ensure this name matches the `profile` specified in your `dbt_project.yml` file (located in `dbt/dbt_project.yml`).

```yaml
# ~/.dbt/profiles.yml

my_dbt_project: # This is the profile name referenced in dbt_project.yml
  target: dev # This is your default target
  outputs:
    dev:
      type: databricks
      host: your-databricks-workspace-host.cloud.databricks.com # e.g., adb-xxxxxxxxxxxxxxxx.xx.azuredatabricks.net
      http_path: /sql/1.0/warehouses/your-dev-sql-warehouse-http-path
      auth_type: oauth # Using OAuth for authentication. This will typically open a browser window for you to log in.
      schema: dbt_penelope_jane # ‼️ replace this with your name, like dbt_randy_pitcher. Trust me
      threads: 16 # Or your preferred number of threads
      catalog: dev_catalog # Optional: if using a separate dev catalog in Unity Catalog

    prod:
      type: databricks
      host: your-databricks-workspace-host.cloud.databricks.com # Often the same host
      http_path: /sql/1.0/warehouses/your-prod-sql-warehouse-http-path
      auth_type: oauth # Using OAuth for authentication
      schema: dbt_prod_schema # Or your main production schema
      threads: 16 # May use more threads in prod
      catalog: prod_catalog # Optional: if using a separate prod catalog in Unity Catalog
```

**Key Configuration Points:**

*   **`host`**: Your Databricks workspace URL (without the `https://`).
*   **`http_path`**: The HTTP Path of your Databricks SQL Warehouse or All-Purpose Cluster.
*   **`auth_type: oauth`**: Specifies that OAuth authentication will be used. For the `dev` target, this will typically open a browser window for you to log in to Databricks. For `prod` targets, especially in CI/CD environments, you'll need to ensure a non-interactive OAuth flow (like M2M using a Service Principal) is configured. Consult the [Databricks OAuth Setup for dbt](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup#oauth-u2m-or-m2m) for detailed instructions.
*   **`schema` (Optional)**: If you want dbt to build objects into a specific schema by default.
*   **`catalog` (Optional for Unity Catalog)**: If you are using Unity Catalog and want to specify an initial catalog. Models can still reference other catalogs.

### Adding Development and Production Targets

It's common practice to have different configurations for your development and production environments. You can define multiple targets within the same profile in your `profiles.yml` as shown in the sample above.