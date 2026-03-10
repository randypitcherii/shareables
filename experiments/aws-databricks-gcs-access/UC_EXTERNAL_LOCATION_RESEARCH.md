# Unity Catalog External Location Research: S3-Compatible GCS Access

## Goal
Trick UC on AWS Databricks into creating an external location that sends S3-compliant requests to GCS instead of AWS S3, using GCS's S3-compatible interoperability API at `storage.googleapis.com` with HMAC keys.

---

## 1. UC Storage Credential Types

Storage credentials are created via CLI/API (not pure SQL — there is no `CREATE STORAGE CREDENTIAL` SQL statement). The CLI/API accepts these credential types:

### On AWS Databricks:
| Credential Type | JSON Field | Auth Mechanism |
|---|---|---|
| **AWS IAM Role** | `aws_iam_role.role_arn` | IAM role ARN, must be self-assuming |
| **Cloudflare R2 API Token** | `cloudflare_api_token` | Account ID + Access Key ID + Secret Access Key |

### On GCP Databricks:
| Credential Type | JSON Field | Auth Mechanism |
|---|---|---|
| **Databricks GCP Service Account** | `databricks_gcp_service_account: {}` | Databricks-managed GCP SA (no user-supplied keys) |
| **AWS IAM Role** (cross-cloud) | `aws_iam_role.role_arn` | For read-only S3 access from GCP |

### On Azure Databricks:
| Credential Type | JSON Field | Auth Mechanism |
|---|---|---|
| **Azure Managed Identity** | `azure_managed_identity` | Managed identity resource ID |
| **Azure Service Principal** | `azure_service_principal` | Directory ID + Application ID + Client Secret |

### Key Finding: No Custom Endpoint Field
**None of the credential types support a custom S3 endpoint.** The `aws_iam_role` type only accepts `role_arn`. There is no `endpoint`, `fs.s3a.endpoint`, or similar field in the storage credential API.

### Key Finding: Cloudflare R2 is the Closest Precedent
R2 uses Access Key ID + Secret Access Key (just like GCS HMAC keys), but it has its own dedicated credential type (`cloudflare_api_token`) and URL scheme (`r2://`). This was added as a first-class integration, not a generic "S3-compatible" option.

---

## 2. UC External Location URL Schemes

The `CREATE EXTERNAL LOCATION` SQL syntax:
```sql
CREATE EXTERNAL LOCATION [IF NOT EXISTS] location_name
  URL 'url_str'
  WITH (STORAGE CREDENTIAL credential_name)
  [COMMENT comment]
```

### Supported URL Schemes by Platform:

| Platform | URL Scheme | Example |
|---|---|---|
| AWS Databricks | `s3://` | `s3://my-bucket/path` |
| AWS Databricks | `r2://` | `r2://my-bucket@account-id.r2.cloudflarestorage.com` |
| GCP Databricks | `gs://` | `gs://my-bucket/path` |
| GCP Databricks | `s3://` | `s3://my-bucket/path` (cross-cloud, read-only) |
| Azure Databricks | `abfss://` | `abfss://container@account.dfs.core.windows.net/path` |

### Key Findings:
- **`url_str` is described as "an absolute URL"** — no documentation of custom endpoint support
- **R2 uses a special scheme**: `r2://bucket@account-id.r2.cloudflarestorage.com` — note how the endpoint is embedded in the URL
- **No `s3a://` scheme** is documented for UC external locations (only `s3://`)
- **No custom endpoint parameter** exists on the `CREATE EXTERNAL LOCATION` statement
- **`gs://` is only valid on GCP Databricks**, not AWS Databricks

---

## 3. Hadoop/S3A Config Overrides

### Standard Hadoop S3A Config:
Hadoop S3A supports per-bucket configuration:
```
spark.hadoop.fs.s3a.endpoint = https://storage.googleapis.com
spark.hadoop.fs.s3a.bucket.BUCKETNAME.endpoint = https://storage.googleapis.com
spark.hadoop.fs.s3a.access.key = GOOG_HMAC_ACCESS_ID
spark.hadoop.fs.s3a.secret.key = GOOG_HMAC_SECRET
```

### Critical Limitation:
**Unity Catalog does NOT respect cluster-level Hadoop filesystem configurations.**

UC manages cloud storage access through its own credential/location objects. When you access data via UC external locations, the `spark.hadoop.fs.s3a.*` settings on the cluster are ignored. UC has its own internal mechanism for translating `s3://` URLs into actual S3 API calls.

This means:
- Setting `fs.s3a.endpoint` on the cluster will NOT redirect UC external location access to GCS
- Setting per-bucket S3A config will NOT affect UC-governed paths
- These Hadoop configs ONLY work for non-UC access patterns (e.g., direct `spark.read` with instance profiles, legacy DBFS mounts)

---

## 4. Databricks REST API Fields

### Storage Credentials Create API (`POST /api/2.1/unity-catalog/storage-credentials`)

Request body fields:
```json
{
  "name": "string",
  "comment": "string",
  "read_only": boolean,
  "skip_validation": boolean,
  "aws_iam_role": {
    "role_arn": "string"
  },
  // OR
  "azure_managed_identity": {
    "access_connector_id": "string",
    "managed_identity_id": "string"  // optional
  },
  // OR
  "azure_service_principal": {
    "directory_id": "string",
    "application_id": "string",
    "client_secret": "string"
  },
  // OR
  "databricks_gcp_service_account": {},
  // OR
  "cloudflare_api_token": {
    "account_id": "string",
    "access_key_id": "string",
    "secret_access_key": "string"
  }
}
```

### External Locations Create API (`POST /api/2.1/unity-catalog/external-locations`)

Request body fields:
```json
{
  "name": "string",
  "url": "string",
  "credential_name": "string",
  "read_only": boolean,
  "comment": "string",
  "skip_validation": boolean
}
```

### Key Finding: No Custom Endpoint Anywhere
- No `endpoint` field on storage credentials
- No `endpoint` field on external locations
- No `properties` or `options` map for arbitrary config
- The Cloudflare R2 integration uses `access_key_id` + `secret_access_key` (same as HMAC keys), but it's locked to R2 infrastructure

---

## 5. Known Workarounds & Cross-Cloud Status

### Official Cross-Cloud Support (as of 2026):
| Source Cloud | Target Storage | Status |
|---|---|---|
| GCP Databricks | AWS S3 | **Supported** (read-only) |
| Azure Databricks | AWS S3 | **Private Preview** |
| AWS Databricks | GCS | **NOT SUPPORTED** |
| AWS Databricks | Azure ADLS | **NOT SUPPORTED** |

### Community Findings:
- **MinIO with UC**: A Databricks community post from March 2024 confirmed: **"Support for S3-compatible storage is not available with Unity Catalog."**
- **Open-source Unity Catalog**: The OSS UC server supports `unity_catalog_aws_endpoint` for S3-compatible services, but this is NOT available in managed Databricks.
- **No documented workaround** exists for using arbitrary S3-compatible endpoints with managed Databricks UC.

### The R2 Precedent:
Cloudflare R2 is the only S3-compatible service with UC support, and it required:
1. A new credential type (`cloudflare_api_token`)
2. A new URL scheme (`r2://`)
3. A new filesystem implementation (`No FileSystem for scheme "r2"` error on old runtimes)
4. DBR 14.3+ / SQL Warehouse 2024.15+

This means adding GCS-via-S3 would likely require a similar first-class integration from Databricks engineering.

---

## 6. GCS S3-Compatible API Details

### Endpoint
`https://storage.googleapis.com`

### Authentication
- Uses HMAC keys (access ID + secret)
- Access ID: 61 chars for service accounts, 24 chars for user accounts
- Secret: 40-char Base64-encoded string
- Supports AWS V4 signing process
- Authorization header uses `AWS4-HMAC-SHA256` with GCS HMAC credentials

### How to Use (from any S3 client)
```python
import boto3
client = boto3.client(
    "s3",
    region_name="auto",
    endpoint_url="https://storage.googleapis.com",
    aws_access_key_id=google_access_key_id,
    aws_secret_access_key=google_access_key_secret,
)
```

### Supported Operations
- Service: `GET` (list buckets)
- Bucket: `PUT`, `GET`, `DELETE`
- Object: `GET`, `POST`, `PUT`, `HEAD`, `DELETE`
- Multipart uploads (with some differences)

### Limitations vs Real S3
| Feature | GCS Behavior |
|---|---|
| Multipart upload final request | Must include customer-supplied encryption key (S3 doesn't) |
| MD5 hashes | Not available for multipart-uploaded objects |
| ETag | Not MD5-based for multipart uploads |
| `x-amz-*` headers | Most `x-amz-*` headers with `x-goog-*` equivalents are supported |
| Storage classes | Must use GCS storage class names in `x-amz-storage-class` |
| Bucket naming | GCS bucket names are globally unique (same as S3) |
| Region | Use `"auto"` as region |
| ACLs | Supports S3 ACL XML syntax in simple migration mode |

---

## 7. Assessment: Can We Trick UC?

### Approach 1: `s3://` URL with IAM Role Credential → GCS
**Will NOT work.** UC uses the IAM role to get temporary credentials from AWS STS, then makes S3 API calls to `s3.amazonaws.com`. There's no way to redirect UC's internal S3 client to `storage.googleapis.com`.

### Approach 2: `r2://` URL with Cloudflare Credential → GCS
**Will NOT work.** The `r2://` scheme routes to Cloudflare R2 endpoints (`*.r2.cloudflarestorage.com`), not arbitrary endpoints. The URL format `r2://bucket@account-id.r2.cloudflarestorage.com` is parsed to extract the R2 account ID.

### Approach 3: Custom URL like `s3://bucket@storage.googleapis.com/path`
**Will NOT work.** The `s3://` scheme expects standard S3 bucket paths. UC doesn't support endpoint embedding in the URL for S3 (only R2 has that pattern).

### Approach 4: Spark Hadoop Config Override
**Will NOT work for UC paths.** UC ignores `spark.hadoop.fs.s3a.*` cluster configs. These only work for non-UC access patterns.

### Approach 5: REST API with Custom Fields
**Will NOT work.** The API has no `endpoint` field, no `properties` map, no way to inject custom configuration.

### What COULD Work (Non-UC Paths):
On classic compute (not serverless), you can bypass UC entirely:
```python
spark.conf.set("spark.hadoop.fs.s3a.bucket.MY_GCS_BUCKET.endpoint", "https://storage.googleapis.com")
spark.conf.set("spark.hadoop.fs.s3a.bucket.MY_GCS_BUCKET.access.key", "GOOG_HMAC_ACCESS_ID")
spark.conf.set("spark.hadoop.fs.s3a.bucket.MY_GCS_BUCKET.secret.key", "GOOG_HMAC_SECRET")

df = spark.read.format("parquet").load("s3a://MY_GCS_BUCKET/path/to/data")
```
This works because it uses the S3A filesystem directly (not UC), which respects Hadoop config. But this gives you zero UC governance.

---

## 8. Bottom Line

**You cannot trick UC into sending S3 requests to GCS.** UC's storage credential and external location system is a closed, opinionated integration with specific cloud providers. It does not expose any mechanism to customize the underlying storage endpoint.

The only paths forward:
1. **Wait for Databricks to add AWS→GCS cross-cloud support** (currently only GCP→S3 read-only exists)
2. **Use non-UC access** (Hadoop S3A config on classic compute) — gives up governance
3. **Lobby Databricks for a generic "S3-compatible" credential type** similar to how R2 was added
4. **Use a GCP Databricks workspace** to natively access GCS, then share data via Delta Sharing to the AWS workspace

---

## Sources
- [Databricks Credentials SQL Reference (AWS)](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-storage-credentials)
- [Databricks Credentials SQL Reference (GCP)](https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-storage-credentials)
- [Databricks External Locations SQL Reference](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-external-locations)
- [CREATE EXTERNAL LOCATION Syntax](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-location)
- [Databricks Cloud Storage Options for UC](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/storage-credentials)
- [Databricks Cloudflare R2 External Location](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/external-locations-r2)
- [Databricks GCP Cross-Cloud S3 (read-only)](https://docs.databricks.com/gcp/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual)
- [Databricks AWS S3 External Location Setup](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual)
- [Storage Credentials REST API](https://docs.databricks.com/api/workspace/storagecredentials/create)
- [GCS S3 Interoperability](https://cloud.google.com/storage/docs/interoperability)
- [GCS Simple Migration from S3](https://cloud.google.com/storage/docs/aws-simple-migration)
- [GCS Full Migration from S3](https://cloud.google.com/storage/docs/migrating)
- [Databricks Community: MinIO UC External Location](https://community.databricks.com/t5/administration-architecture/setup-unity-catalog-external-location-to-minio/td-p/49895)
