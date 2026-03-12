# DuckDB + Databricks Unity Catalog via Iceberg REST Catalog

**Research date:** 2026-03-11
**Sources:** DuckDB docs (stable), Databricks AWS/Azure docs (updated Feb 2026), GitHub duckdb/duckdb-iceberg issues, DuckDB blog posts (Nov/Dec 2025)

---

## TL;DR

DuckDB's `iceberg` extension (v1.4.0+) supports attaching Iceberg REST Catalogs with full read/write SQL. Unity Catalog exposes an Iceberg REST endpoint at `/api/2.1/unity-catalog/iceberg-rest`. **The critical blocker for most Databricks users: DuckDB's Iceberg REST Catalog support currently only works with S3 or S3 Tables as the backing storage.** Azure ADLS and GCS are not yet supported. AWS-backed Unity Catalog workspaces on S3 should work.

---

## 1. Endpoint URL Format

Databricks Unity Catalog exposes the Iceberg REST API at:

```
https://<workspace-url>/api/2.1/unity-catalog/iceberg-rest
```

**Critical:** The workspace URL must include the workspace ID. Without it, API requests return a `303` redirect to a login page.

Examples of correct workspace URLs:
- AWS: `https://cust-success.cloud.databricks.com/?o=6280049833385130`
- Azure: `https://adb-1234567890123456.12.azuredatabricks.net`

The `warehouse` parameter is the Unity Catalog **catalog name** (not a schema).

---

## 2. DuckDB Configuration

### Install and load the extension

```sql
INSTALL iceberg;
LOAD iceberg;
LOAD httpfs;  -- required for remote storage access
```

Always update the extension to get the latest fixes:

```sql
UPDATE EXTENSIONS;
```

### Option A: PAT token (simplest)

```sql
CREATE SECRET uc_iceberg_secret (
    TYPE iceberg,
    TOKEN 'dapi...'  -- Databricks PAT token
);

ATTACH 'my_unity_catalog' AS uc_catalog (
    TYPE iceberg,
    SECRET uc_iceberg_secret,
    ENDPOINT 'https://<workspace-url>/api/2.1/unity-catalog/iceberg-rest'
);
```

### Option B: OAuth2 (service principal)

OAuth token endpoint for Databricks: `https://<workspace-url>/oidc/v1/token`

```sql
CREATE SECRET uc_iceberg_secret (
    TYPE iceberg,
    CLIENT_ID '<service-principal-client-id>',
    CLIENT_SECRET '<service-principal-client-secret>',
    OAUTH2_SERVER_URI 'https://<workspace-url>/oidc/v1/token',
    OAUTH2_SCOPE 'all-apis'
);

ATTACH 'my_unity_catalog' AS uc_catalog (
    TYPE iceberg,
    SECRET uc_iceberg_secret,
    ENDPOINT 'https://<workspace-url>/api/2.1/unity-catalog/iceberg-rest'
);
```

### Option C: Inline ATTACH with OAuth2 (no separate CREATE SECRET)

```sql
ATTACH 'my_unity_catalog' AS uc_catalog (
    TYPE iceberg,
    ENDPOINT 'https://<workspace-url>/api/2.1/unity-catalog/iceberg-rest',
    CLIENT_ID '<client-id>',
    CLIENT_SECRET '<client-secret>',
    OAUTH2_SERVER_URI 'https://<workspace-url>/oidc/v1/token'
);
```

### Credential vending (recommended for AWS)

Unity Catalog supports credential vending — the catalog hands DuckDB temporary credentials for the underlying S3 storage. This is the default (`ACCESS_DELEGATION_MODE = 'vended_credentials'`), so you don't need to configure your own AWS credentials separately.

```sql
ATTACH 'my_unity_catalog' AS uc_catalog (
    TYPE iceberg,
    SECRET uc_iceberg_secret,
    ENDPOINT 'https://<workspace-url>/api/2.1/unity-catalog/iceberg-rest',
    ACCESS_DELEGATION_MODE 'vended_credentials'  -- this is already the default
);
```

If credential vending doesn't work or isn't supported, disable it and provide your own storage credentials:

```sql
-- Create an S3 secret separately
CREATE SECRET s3_creds (
    TYPE s3,
    KEY_ID 'AKIA...',
    SECRET '...',
    REGION 'us-east-1'
);

ATTACH 'my_unity_catalog' AS uc_catalog (
    TYPE iceberg,
    SECRET uc_iceberg_secret,
    ENDPOINT 'https://<workspace-url>/api/2.1/unity-catalog/iceberg-rest',
    ACCESS_DELEGATION_MODE 'none'
);
```

### Verify the connection

```sql
SHOW ALL TABLES;

SELECT * FROM uc_catalog.my_schema.my_table LIMIT 10;
```

---

## 3. All ATTACH / CREATE SECRET Options

| Parameter | Type | Default | Notes |
|-----------|------|---------|-------|
| `ENDPOINT` | VARCHAR | NULL | Full REST catalog URL |
| `SECRET` | VARCHAR | NULL | Named secret to use |
| `CLIENT_ID` | VARCHAR | NULL | OAuth2 client ID |
| `CLIENT_SECRET` | VARCHAR | NULL | OAuth2 client secret |
| `OAUTH2_SERVER_URI` | VARCHAR | NULL | OAuth2 token endpoint |
| `OAUTH2_SCOPE` | VARCHAR | NULL | OAuth2 scope (e.g., `all-apis`) |
| `OAUTH2_GRANT_TYPE` | VARCHAR | NULL | OAuth2 grant type (SECRET only) |
| `AUTHORIZATION_TYPE` | VARCHAR | `OAUTH2` | Use `none` for no auth |
| `ACCESS_DELEGATION_MODE` | VARCHAR | `vended_credentials` | `vended_credentials` or `none` |
| `DEFAULT_REGION` | VARCHAR | NULL | AWS region for storage |
| `SUPPORT_NESTED_NAMESPACES` | BOOLEAN | true | Set false if catalog doesn't support |
| `SUPPORT_STAGE_CREATE` | BOOLEAN | false | Unity Catalog may need true |
| `PURGE_REQUESTED` | BOOLEAN | true | Send purge flag on DROP TABLE |

---

## 4. Supported Operations (DuckDB Iceberg REST, v1.4.2+)

| Operation | Supported | Notes |
|-----------|-----------|-------|
| `SELECT` | Yes | Full read support |
| `INSERT INTO` | Yes | Added in v1.4.0 |
| `UPDATE` | Yes | Added in v1.4.2 |
| `DELETE` | Yes | Added in v1.4.2 |
| `CREATE TABLE` | Yes | Standard SQL syntax |
| `DROP TABLE` | Yes | |
| `CREATE SCHEMA` | Yes | |
| `DROP SCHEMA` | Yes | |
| `COPY FROM DATABASE` | Yes | Deep copy between DuckDB/Iceberg |
| `MERGE INTO` | **No** | Not supported |
| `ALTER TABLE` | **No** | Not supported |
| Time travel (`AT VERSION =>`) | Yes | Snapshot ID or timestamp |

### What's supported via Databricks Unity Catalog (server-side)

| Table type | Read via Iceberg REST | Write via Iceberg REST |
|------------|----------------------|----------------------|
| Managed Iceberg | Yes | Yes |
| Foreign Iceberg | Yes | No |
| Managed Delta (UniForm enabled) | Yes | No |
| External Delta (UniForm enabled) | Yes | No |

**Note:** Delta tables must have UniForm/Iceberg reads explicitly enabled to be visible via the Iceberg REST endpoint. See Databricks docs on `ALTER TABLE ... SET TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')`.

---

## 5. Predicate Pushdown

DuckDB does support predicate pushdown for Iceberg tables, including through the REST catalog path:

- **Manifest-level file pruning:** DuckDB uses Iceberg manifest statistics to skip entire data files that can't match a filter. This is the primary pushdown mechanism.
- **Row-group level pruning:** Within parquet files, row group min/max statistics are used to skip row groups.
- **Partition pruning:** Partitioned tables get partition-based file elimination.

**Known issue (as of early 2026):** There is an open GitHub issue ([#638](https://github.com/duckdb/duckdb-iceberg/pull/638)) noting that predicate pushdown is incomplete for some filter types, particularly `OR` conditions. A PR adding extended filter pruning was open as of Jan 2026 but not yet merged.

**Debugging pushdown:**

```sql
CALL enable_logging('Iceberg');

SELECT * FROM uc_catalog.my_schema.my_table WHERE some_col = 'value';

SELECT * FROM duckdb_logs()
WHERE type = 'Iceberg'
  AND (message LIKE '%data_file%' OR message LIKE '%manifest_file%');
```

**Also known:** `iceberg_scan predicate pushdown incorrectly prunes files when manifest stats (null_value_counts, value_counts) are NULL` — open bug in duckdb-iceberg issues.

---

## 6. Limitations and Known Issues

### Storage backend limitation (CRITICAL)

> "Reading from Iceberg REST Catalogs backed by remote storage that is not S3 or S3 Tables is not yet supported."
> — DuckDB docs (stable, as of March 2026)

This means:
- **AWS-backed Unity Catalog (S3):** Should work.
- **Azure-backed Unity Catalog (ADLS):** Not supported. DuckDB can talk to the REST catalog API but cannot read the actual parquet/avro files from ADLS.
- **GCP-backed Unity Catalog (GCS):** Not supported for the same reason.

This is the single largest practical limitation for Databricks users not on AWS.

### Other known issues

- `MERGE INTO` is not supported.
- `ALTER TABLE` is not supported.
- The `unity_catalog` extension (Delta-based path, separate from `iceberg` extension) has more limited write support (no DELETE/UPDATE).
- Vended credentials with custom S3 endpoints (e.g., MinIO) have a known bug where the custom endpoint is ignored during write operations (issue #594, reported Nov 2025, DuckDB v1.4.2).
- Foreign Iceberg tables in Unity Catalog are not automatically refreshed — you must run `REFRESH FOREIGN TABLE` before reading.
- Credential vending is not supported for Foreign Iceberg tables in Unity Catalog.
- The Databricks Iceberg REST API (new endpoint) is in **Public Preview** as of Databricks Runtime 16.4 LTS. The older legacy read-only endpoint (`/api/2.1/unity-catalog/iceberg`) still exists but is considered legacy.

### Two different DuckDB extensions for Unity Catalog

There are two separate DuckDB extensions that can connect to Unity Catalog — they use different protocols:

| Extension | Protocol | Format | Write support |
|-----------|----------|--------|--------------|
| `iceberg` | Iceberg REST Catalog API | Iceberg | Full (INSERT/UPDATE/DELETE) |
| `unity_catalog` | Unity Catalog REST API | Delta Lake | Limited (INSERT only, no DELETE/UPDATE) |

For Iceberg tables or Delta+UniForm tables, prefer the `iceberg` extension path documented here.

---

## 7. Version Requirements

| Component | Minimum Version | Notes |
|-----------|----------------|-------|
| DuckDB | 1.4.0 | First version with Iceberg REST write support |
| DuckDB | 1.4.2 | DELETE and UPDATE support added |
| `iceberg` extension | Auto-updated | Run `UPDATE EXTENSIONS` to get latest |
| Databricks Runtime | 16.4 LTS | Required for new Iceberg REST catalog API (Public Preview) |

The `iceberg` extension is a "core extension" that updates independently of DuckDB. Always run `UPDATE EXTENSIONS` before testing to get the latest fixes.

DuckDB 1.4.0 is an LTS release with community support until September 2026.

---

## 8. HTTP Request Debugging

To see exactly what requests DuckDB is making to the REST catalog and storage:

```sql
-- Enable HTTP logging
SET enable_http_logging = true;

-- Run your query
SELECT * FROM uc_catalog.my_schema.my_table LIMIT 5;

-- View the log
SELECT * FROM duckdb_logs() WHERE type = 'HTTP';
```

---

## 9. PyIceberg Config (for reference/comparison)

For validating that the UC endpoint itself works before trying DuckDB, use PyIceberg:

```yaml
# ~/.pyiceberg.yaml
catalog:
  unity_catalog:
    uri: https://<workspace-url>/api/2.1/unity-catalog/iceberg-rest
    warehouse: <uc-catalog-name>
    token: <pat-token>
```

```python
from pyiceberg.catalog import load_catalog
catalog = load_catalog("unity_catalog")
catalog.list_namespaces()
```

---

## 10. Recommended Approach for Experimentation

Given the storage backend limitation, the recommended path for this experiment:

1. **If on AWS:** Use the `iceberg` extension with PAT token + vended credentials. This is the cleanest path.
2. **If on Azure/GCP:** The `iceberg` extension REST path won't work for reading data files. Options:
   - Use the `unity_catalog` extension (Delta protocol) instead — it uses a different code path that does support Azure storage.
   - Or test only catalog-level operations (table listing, schema inspection) without actually reading data.

For AWS, the minimum working config:

```sql
INSTALL iceberg;
LOAD iceberg;
UPDATE EXTENSIONS;

CREATE SECRET uc_secret (
    TYPE iceberg,
    TOKEN 'dapi<your-pat-token>'
);

-- workspace URL must include ?o=<workspace-id> for AWS
ATTACH 'my_uc_catalog_name' AS uc (
    TYPE iceberg,
    SECRET uc_secret,
    ENDPOINT 'https://my-workspace.cloud.databricks.com/?o=1234567890/api/2.1/unity-catalog/iceberg-rest'
);

SHOW ALL TABLES;
SELECT * FROM uc.my_schema.my_table LIMIT 10;
```

---

## Sources

- [DuckDB Iceberg REST Catalogs docs](https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs.html)
- [DuckDB Iceberg writes blog post (Nov 2025)](https://duckdb.org/2025/11/28/iceberg-writes-in-duckdb.html)
- [DuckDB 1.4.0 announcement (Sep 2025)](https://duckdb.org/2025/09/16/announcing-duckdb-140.html)
- [Databricks: Access tables from Apache Iceberg clients (AWS)](https://docs.databricks.com/aws/en/external-access/iceberg)
- [Databricks: Access tables from Apache Iceberg clients (Azure)](https://learn.microsoft.com/en-us/azure/databricks/external-access/iceberg)
- [GitHub: duckdb/duckdb-iceberg issues](https://github.com/duckdb/duckdb-iceberg/issues)
- [GitHub PR #638: Add extended filter pruning optimizations (Jan 2026)](https://github.com/duckdb/duckdb-iceberg/pull/638)
- [GitHub issue #594: Custom S3 endpoint not used with vended credentials](https://github.com/duckdb/duckdb-iceberg/issues/594)
