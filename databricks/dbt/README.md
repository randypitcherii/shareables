# dbt for Databricks - Why should you care?

## The Problem
Tired of this data engineering chaos?
- SQL spaghetti scattered across unversioned notebooks?
- Zero confidence because you can't test your data transformations?
- Endless copy-pasting leading to a fragile, unmaintainable mess?
- Undocumented data models that nobody understands?
- Manual, error-prone deployments slowing you down?

## The Solution: dbt
dbt is your secret weapon. It supercharges your Databricks workflows:
- **Leverages Databricks Compute, Saves Money**: dbt pushes all transformation logic directly into Databricks, efficiently using its parallel compute capabilities to reduce costs.
- **Version-Controlled & Modular SQL**: All your SQL lives in git. Build reusable, maintainable data models with macros and `ref()` functions.
- **Rich Ecosystem of Packages**: Tap into a vast library of pre-built dbt packages for data quality, transformations, migrations, and more, accelerating your development.
- **Testing & Validation**: Ensure your data meets expectations with built-in testing frameworks.
- **Automatic Documentation**: Generate comprehensive, up-to-date documentation for your entire data landscape.
- **CI/CD Ready**: Automate your deployments and move code seamlessly between environments.
- **Widely Adopted & Easy to Learn**: dbt is a popular, well-documented tool with a gentle learning curve. Plus, LLMs are surprisingly good at generating dbt code and answering your questions!

## What dbt is NOT
- dbt is NOT the same as dbt Cloud (the paid SaaS product)
  - `pip install dbt-databricks` and you're done
  - in fact, `dbt-databricks` is ENTIRELY built by DATABRICKS! Using `dbt-databricks` is like using `dbutils`, `databricks-sdk`, or `databricks-connect`
- dbt is NOT an ETL/ELT tool like Fivetran (it's the T in ELT)
- dbt is NOT a data warehouse or legacy ETL tool
  - dbt can not do compute. It must use Databricks to do any data processing.
- dbt is NOT taking compute away from Databricks - it USES Databricks compute

## What dbt IS

dbt is a command-line tool that:
1. Takes simple SQL select statements
2. Builds parellel deployments by default
3. Scams people into using version control, CICD, and testing
4. Manages your dev/test/prod/etc.. isolation automatically
5. Tests your data AND your transformations
6. Generates lineage documentation (and pushes it into databricks UC)

It's a complete workflow for managing data transformations in your warehouse.

It is the paved path that databricks users are desperate for.

## Fine, now what?
- follow the setup below and see for yourself
- it takes so little time you can even do it in a github codespace and not download anything


# dbt Project Setup

This guide provides instructions for setting up your [dbt](https://docs.getdbt.com/) environment to work with this project, focusing on connecting to Databricks.

## 1. Python Environment and dbt Installation

You'll need Python installed. Then, you can use either `uv` (preferred) or `poetry` to manage your dbt installation and dependencies.

### Using `uv`

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
