# DuckDB + Delta Tables via Databricks Unity Catalog REST (Delta-Sharing Path)

> Research date: 2026-03-11
> Focus: UC REST / delta-sharing path — NOT the Iceberg REST path

---

## Key Finding Up Front

**DuckDB has no native delta-sharing protocol client.** The `delta` extension (`delta_scan`) reads Delta tables by directly accessing the underlying Parquet files on cloud storage (S3, ADLS, GCS). It does NOT speak the Delta Sharing REST protocol.

To use the Delta Sharing REST path from DuckDB you must:
1. Use the Python `delta-sharing` library to resolve files via the Delta Sharing server, then
2. Hand those presigned URLs or file paths to DuckDB's Parquet reader — manually or via a wrapper.

There is **no DuckDB `delta_sharing` extension** as of March 2026 (only the `delta` extension exists).

---

## 1. The Two Distinct Approaches

### Approach A: `delta_scan` (delta extension — direct cloud storage)

- Uses [delta-kernel-rs](https://github.com/delta-incubator/delta-kernel-rs) under the hood
- Reads Delta tables by accessing the `_delta_log/` directory directly on the storage backend
- Requires DuckDB to have cloud storage credentials (not a Databricks token)
- **Does not go through Unity Catalog at all** — bypasses UC entirely
- Supports blind-insert writes (append-only)

### Approach B: Delta Sharing Protocol (UC REST path)

- UC exposes a Delta Sharing server at `<workspace-url>/api/2.0/delta-sharing/metastores/<metastore-id>/`
- Clients authenticate with a bearer token (PAT or short-lived token from a credential file)
- UC returns presigned cloud storage URLs for the underlying Parquet files
- **Read-only by protocol design** — the Delta Sharing spec has no write operations
- DuckDB has no built-in client for this protocol; you need a Python shim

---

## 2. Delta Sharing Protocol and UC Endpoint

### UC Delta Sharing Endpoint URL Format

```
https://<workspace-url>/api/2.0/delta-sharing/metastores/<metastore-id>/
```

This is the **Delta Sharing REST server** endpoint. The credential file downloaded from UC contains this URL plus a bearer token.

### Credential File Format (profile file)

```json
{
  "shareCredentialsVersion": 1,
  "endpoint": "https://<workspace-url>/api/2.0/delta-sharing/metastores/<metastore-id>/",
  "bearerToken": "<pat-or-short-lived-token>",
  "expirationTime": "2026-12-31T00:00:00.0Z"
}
```

- `shareCredentialsVersion`: currently `1`
- `endpoint`: the Delta Sharing server base URL
- `bearerToken`: PAT or short-lived token — PATs work fine for testing
- `expirationTime`: optional; if absent, token is treated as non-expiring

### Key REST Endpoints Under That Base URL

```bash
# List shares
GET <endpoint>/shares
Authorization: Bearer <bearerToken>

# List schemas in a share
GET <endpoint>/shares/<share>/schemas

# List tables in a schema
GET <endpoint>/shares/<share>/schemas/<schema>/tables

# Query table metadata
GET <endpoint>/shares/<share>/schemas/<schema>/tables/<table>/metadata

# Read table data (returns presigned URLs for Parquet files)
POST <endpoint>/shares/<share>/schemas/<schema>/tables/<table>/query
Content-Type: application/json
{
  "predicateHints": ["date >= '2024-01-01'"],
  "jsonPredicateHints": "...",
  "limitHint": 1000
}
```

### UC-Specific: Iceberg Endpoint Also Exposed

The credential file also exposes an Iceberg REST endpoint:
```
<workspace-url>/api/2.0/delta-sharing/metastores/<metastore-id>/iceberg
```
This is separate from the Delta Sharing endpoint and is for Iceberg clients — not covered here.

---

## 3. Authentication

### For Open Sharing (non-Databricks recipients)

Authentication uses a **bearer token** in the credential file, which can be:
- A Databricks **Personal Access Token (PAT)** — long-lived, simpler for dev
- A short-lived token from OIDC federation — preferred for production

### For Direct UC REST (without formal share/recipient setup)

You can call the Delta Sharing REST API directly using a PAT:
```bash
curl -X GET \
  "https://<workspace-url>/api/2.0/delta-sharing/metastores/<metastore-id>/shares" \
  -H "Authorization: Bearer <pat-token>"
```

Note: A formal "recipient" object must be created in UC, and that recipient must be granted access to shares. A PAT by itself doesn't automatically expose all tables — the UC governance layer controls what the recipient can see.

---

## 4. Operations Supported

### Delta Sharing Protocol

| Operation | Supported |
|-----------|-----------|
| Read (SELECT) | Yes |
| Write (INSERT/UPDATE/DELETE) | **No — protocol is read-only by design** |
| Schema discovery | Yes (list shares/schemas/tables) |
| Change Data Feed | Yes (streaming extension of protocol) |
| Create tables | No |
| Drop tables | No |

The Delta Sharing protocol spec has no write endpoints. The server returns presigned URLs to Parquet files for download. Writes are impossible through this path.

### DuckDB `delta` Extension (direct storage path)

| Operation | Supported |
|-----------|-----------|
| Read (SELECT) | Yes |
| Append (blind insert) | Yes — limited |
| Merge/Update/Delete | No |
| Create Delta table | No (must pre-exist) |
| Predicate pushdown | Yes |
| Projection pushdown | Yes |
| Partition pruning | Yes |

---

## 5. How to Actually Query UC Delta Tables from DuckDB

### Method 1: Direct Storage Access via `delta_scan` (bypasses UC)

This is the most functional approach for DuckDB. Requires direct cloud storage credentials.

```sql
-- AWS: authenticate with instance profile or explicit credentials
CREATE SECRET my_aws_secret (
    TYPE S3,
    PROVIDER CREDENTIAL_CHAIN
);
-- or explicit:
CREATE SECRET my_aws_secret (
    TYPE S3,
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    REGION 'us-east-1'
);

-- Read the table directly from its storage path
FROM delta_scan('s3://my-bucket/path/to/delta/table');

-- Azure
CREATE SECRET (
    TYPE AZURE,
    PROVIDER CREDENTIAL_CHAIN,
    CHAIN 'cli',
    ACCOUNT_NAME 'mystorageaccount'
);
FROM delta_scan('abfss://container@account.dfs.core.windows.net/path/to/table');
```

To find a UC table's storage path:
```sql
-- In Databricks SQL
DESCRIBE EXTENDED catalog.schema.table_name;
-- Look for "Location" field — this is the storage path to pass to delta_scan
```

**Limitation:** DuckDB needs direct cloud storage credentials, not just a Databricks token. UC credential vending is not supported by `delta_scan`.

### Method 2: Python Bridge via `delta-sharing` Library

Use the Python delta-sharing library to resolve the presigned file URLs, then query from DuckDB.

```python
import delta_sharing
import duckdb

# Point to your credential file
profile_file = "/path/to/credential.share"

client = delta_sharing.SharingClient(profile_file)

# List available shares
shares = client.list_shares()

# Load a table as pandas (or get file URLs)
table_url = f"{profile_file}#<share>.<schema>.<table>"
df = delta_sharing.load_as_pandas(table_url)

# Query via DuckDB
conn = duckdb.connect()
result = conn.execute("SELECT * FROM df WHERE col > 100").fetchdf()
```

**Limitation:** This pulls all data to memory first via pandas. No pushdown.

### Method 3: Python Bridge with Presigned URLs (more efficient)

```python
import delta_sharing
import duckdb

profile_file = "/path/to/credential.share"
table_url = f"{profile_file}#<share>.<schema>.<table>"

# Get presigned URLs for files
files = delta_sharing.load_as_pandas(
    table_url,
    # Use the underlying client to get file references instead
)

# Better: use the REST API directly
import requests, json

with open(profile_file) as f:
    creds = json.load(f)

endpoint = creds["endpoint"]
token = creds["bearerToken"]

# Query table to get presigned URLs
resp = requests.post(
    f"{endpoint}/shares/<share>/schemas/<schema>/tables/<table>/query",
    headers={"Authorization": f"Bearer {token}"},
    json={"predicateHints": ["date = '2024-01-01'"]}
)

# Response is NDJSON: each line is a JSON action (protocol, metadata, or file)
# File lines contain presigned URLs
for line in resp.text.strip().split("\n"):
    action = json.loads(line)
    if "file" in action:
        presigned_url = action["file"]["url"]
        # Pass to DuckDB
        conn = duckdb.connect()
        conn.execute(f"SELECT * FROM parquet_scan('{presigned_url}')")
```

**Note:** This is essentially manual implementation of the delta-sharing client. The Python library handles this for you.

---

## 6. Predicate Pushdown

### `delta_scan` (direct storage)

Full predicate pushdown support:
- **Row-group skipping** based on Parquet min/max statistics
- **Partition pruning** based on Delta partition info
- **Projection pushdown** (column pruning)
- Works automatically — no special SQL syntax needed

```sql
-- DuckDB automatically pushes this predicate to the storage layer
SELECT * FROM delta_scan('s3://bucket/table')
WHERE event_date = '2024-01-01'
  AND region = 'us-east-1';
```

### Delta Sharing Protocol Path

- The server supports `predicateHints` (SQL string) and `jsonPredicateHints` (structured JSON) in the query request body
- These are **best-effort hints only** — the server MAY ignore them
- The server returns presigned Parquet file URLs; the client is still responsible for in-memory filtering
- Partition pruning can work (server filters which files to return based on partition predicates)
- Row-group skipping does NOT happen at the server — the client must do it
- When using the Python bridge, DuckDB's Parquet reader can do row-group skipping on presigned URLs

---

## 7. Write Access

### Delta Sharing Protocol

**No writes possible.** The spec defines no write endpoints. This is fundamental to the protocol, not a temporary limitation.

### `delta_scan` (direct storage)

Limited "blind insert" (append) is supported:

```sql
-- Append rows to an existing Delta table
INSERT INTO delta_scan('s3://bucket/path/to/delta/table')
SELECT * FROM source_table;

-- Or use COPY
COPY (SELECT * FROM source) TO delta_scan('s3://bucket/path/to/delta/table');
```

**Caveats for blind inserts:**
- No conflict detection — concurrent writers can corrupt the table
- No schema enforcement at the DuckDB level
- No support for DELETE, UPDATE, or MERGE
- Writing to a UC-governed table this way bypasses UC governance entirely

### If You Need Real Write Access from DuckDB to Databricks

Options (none are clean):
1. **Databricks JDBC/ODBC** — write via SQL through the Databricks SQL endpoint (not DuckDB native Delta writes)
2. **Write Parquet files to the storage location, then `REPAIR TABLE` in Databricks** — hacky
3. **Use delta-rs (Python)** — the `deltalake` Python package can write Delta tables, then DuckDB reads them
4. **Iceberg REST path** — DuckDB's Iceberg extension supports full read/write against UC's Iceberg REST API (a different extension, but worth considering)

---

## 8. Version Requirements

| Component | Minimum Version |
|-----------|----------------|
| DuckDB | 0.10.3+ (for `delta` extension) |
| `delta` extension | Auto-loaded from DuckDB 0.10.3+ |
| delta-kernel-rs | Bundled with the extension |
| Python delta-sharing lib | Latest (`pip install delta-sharing`) |
| Databricks Runtime | Any (for generating credential files) |

**Supported DuckDB platforms for `delta` extension:**
- Linux AMD64 / ARM64
- macOS Intel / Apple Silicon (osx_amd64, osx_arm64)
- Windows AMD64

---

## 9. Relationship: Delta Sharing Protocol vs UC REST vs Direct Delta

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Unity Catalog                                │
│                                                                     │
│  ┌──────────────┐  ┌───────────────────┐  ┌───────────────────┐   │
│  │ Delta Tables │  │  Delta Sharing    │  │  Iceberg REST     │   │
│  │  (storage)   │  │  Server           │  │  Catalog API      │   │
│  │ s3://bucket/ │  │ /api/2.0/delta-   │  │ /api/2.1/unity-   │   │
│  │ _delta_log/  │  │  sharing/...      │  │  catalog/iceberg- │   │
│  └──────┬───────┘  └────────┬──────────┘  │  rest             │   │
│         │                   │             └────────┬──────────┘   │
└─────────┼───────────────────┼──────────────────────┼──────────────┘
          │                   │                      │
          ▼                   ▼                      ▼
   delta_scan()         delta-sharing           DuckDB Iceberg
   (DuckDB delta        Python library          extension
    extension)          (read-only)             (read + write)

   Direct cloud         Presigned URLs          Presigned URLs
   credentials          via protocol            via credential
   required             (PAT in profile)        vending

   Read + limited       Read only               Read + write
   append writes
```

**The delta-sharing protocol sits in the middle** — it's the protocol that UC's built-in Delta Sharing server speaks. It is a **different protocol from the direct Delta Lake format** that `delta_scan` reads. The Delta Sharing protocol downloads Parquet files via presigned URLs; the Delta format is the versioned transaction log + Parquet on storage.

---

## 10. Tradeoffs and Limitations Summary

| Factor | `delta_scan` (direct) | Delta Sharing via Python | Iceberg REST (DuckDB) |
|--------|----------------------|--------------------------|----------------------|
| DuckDB integration | Native | Python bridge | Native (attach) |
| Auth | Cloud storage creds | PAT/OIDC bearer token | PAT/OAuth |
| Predicate pushdown | Full | Best-effort hints | Full |
| Write support | Blind append | None | Full CRUD |
| UC governance | Bypassed | Enforced | Enforced |
| Credential vending | Not supported | Built into protocol | Supported |
| Complexity | Low | Medium | Low |
| Recommended for | Dev/exploration | Sharing scenarios | Production reads/writes |

### Key Limitations of Delta Sharing Path

1. **No DuckDB native client** — must use Python library as intermediary
2. **Read-only** — protocol has no write operations, period
3. **Predicate pushdown is server-side hints only** — no guarantee of filtering
4. **Credential file management** — expiring tokens require rotation
5. **Performance** — loading through pandas materializes full dataset in memory
6. **Schema drift** — credential files point to shares, not live tables; schema changes require recipient refresh

### Key Limitations of `delta_scan` (direct storage)

1. **No UC auth** — must have cloud storage credentials separately
2. **Bypasses UC governance** — column masking, row filters, audit logs don't apply
3. **Blind insert only** — no MERGE, UPDATE, DELETE
4. **No ATTACH/catalog** — must use `delta_scan()` function, no SQL catalog browsing

---

## 11. Recommended Configuration for Common Scenarios

### Scenario A: Query UC tables from DuckDB (dev, you have storage access)

```sql
INSTALL delta;
LOAD delta;

-- Create cloud storage secret
CREATE SECRET (
    TYPE S3,
    PROVIDER CREDENTIAL_CHAIN
);

-- Get the table location from Databricks first:
-- DESCRIBE EXTENDED catalog.schema.my_table -> Location: s3://...

FROM delta_scan('s3://my-databricks-bucket/path/to/catalog/schema/my_table');
```

### Scenario B: Query a Delta Sharing share from DuckDB (UC governance enforced)

```python
# Install: pip install delta-sharing duckdb
import delta_sharing, duckdb

profile = "path/to/profile.share"
# Profile file obtained from: Databricks UI -> Delta Sharing -> Recipient -> Download credentials

table_url = f"{profile}#my_share.my_schema.my_table"
df = delta_sharing.load_as_pandas(table_url)

conn = duckdb.connect()
conn.register("shared_table", df)
result = conn.execute("SELECT * FROM shared_table WHERE col > 100").df()
```

### Scenario C: Append data to a UC Delta table from DuckDB

```sql
-- Requires direct cloud storage access (bypasses UC)
INSERT INTO delta_scan('s3://bucket/path/to/table')
SELECT * FROM read_parquet('local_data.parquet');
```

### Scenario D: Full read/write with UC governance (use Iceberg path instead)

```sql
-- This is the Iceberg REST path, not Delta — but it's the only way to get
-- full CRUD with UC governance from DuckDB
INSTALL iceberg;
LOAD iceberg;

CREATE SECRET uc_secret (
    TYPE ICEBERG,
    CLIENT_ID '<oauth-client-id>',
    CLIENT_SECRET '<oauth-client-secret>',
    OAUTH2_SERVER_URI 'https://<workspace-url>/oidc/v1/token'
);

ATTACH 'https://<workspace-url>/api/2.1/unity-catalog/iceberg-rest'
    AS uc_catalog (
        TYPE ICEBERG,
        SECRET 'uc_secret',
        WAREHOUSE '<uc-catalog-name>'
    );

-- Now full SQL access
SELECT * FROM uc_catalog.my_schema.my_table;
INSERT INTO uc_catalog.my_schema.my_table VALUES (...);
```

---

## 12. Current State Assessment (March 2026)

- **No native DuckDB delta-sharing extension exists** and none is on the public roadmap
- The `delta` extension is actively maintained and works well for direct storage access
- Delta Sharing via Python is workable but not ergonomic for SQL-first workflows
- **The practical recommended path for UC + DuckDB is the Iceberg REST catalog** (DuckDB's Iceberg extension, using `/api/2.1/unity-catalog/iceberg-rest`) which gives full read/write with UC governance — this is the path Databricks is actively investing in for external client access
- If you specifically need the Delta format (not Iceberg) and have storage access, `delta_scan` is solid
- Write access to UC Delta tables from DuckDB via UC-governed paths does not exist

---

## Sources

- [DuckDB Delta Extension Docs](https://duckdb.org/docs/stable/core_extensions/delta.html)
- [DuckDB Delta Extension GitHub](https://github.com/duckdb/duckdb-delta)
- [Delta Sharing Protocol Spec](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md)
- [Delta Sharing GitHub](https://github.com/delta-io/delta-sharing)
- [Databricks Delta Sharing Docs (open sharing, for recipients)](https://docs.databricks.com/aws/en/delta-sharing/read-data-open)
- [Databricks Delta Sharing Docs (for providers)](https://docs.databricks.com/aws/en/delta-sharing/share-data-open)
- [Databricks Delta Sharing Index](https://docs.databricks.com/aws/en/delta-sharing/index)
