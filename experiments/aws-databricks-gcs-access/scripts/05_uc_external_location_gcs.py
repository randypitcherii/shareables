"""
Approach 5: Unity Catalog External Location Tricks for GCS Access

Tests whether UC can be tricked into creating external locations that route
S3-compliant requests to GCS's S3-compatible endpoint. Tries every plausible
config combination.

Research says this will NOT work — UC's credential types and URL schemes are
hardcoded with no custom endpoint support. But we test anyway to document
the exact failure modes.

Tests:
  1. s3:// URL with IAM role credential pointing at GCS bucket
  2. r2:// URL format pointing at GCS endpoint instead of Cloudflare
  3. s3:// URL with endpoint embedded in path (s3://bucket@storage.googleapis.com)
  4. Storage credential with HMAC keys via Cloudflare R2 credential type
  5. gs:// URL (only valid on GCP Databricks, testing from AWS)

Requires:
  - Existing GCS secrets in 'gcs-experiment' scope (from Terraform setup)
  - An IAM role ARN for the workspace (auto-detected from instance profile)
"""

from _common import (
    BUCKET,
    get_secret,
    get_spark_and_dbutils,
    print_header,
    print_summary,
)

LOC_PREFIX = "gcs_trick"
CRED_PREFIX = "gcs_trick_cred"

# Existing IAM role credential we have permissions on
EXISTING_CRED = "rpw_aws_fe_sandbox_storage_credential"


def cleanup_location(w, name_suffix):
    """Delete external location if it exists."""
    try:
        w.external_locations.delete(f"{LOC_PREFIX}_{name_suffix}", force=True)
    except Exception:
        pass


def cleanup_credential(w, name):
    """Delete storage credential if it exists."""
    try:
        w.storage_credentials.delete(name, force=True)
    except Exception:
        pass


def test_s3_url_with_iam_role(spark, dbutils):
    """Test 1: s3:// URL pointing at GCS bucket name with existing IAM role.

    Hypothesis: UC will try to reach s3://dbx-gcs-access-experiment via AWS S3,
    which will fail because that bucket doesn't exist in S3.
    """
    print_header("Test 1: s3:// URL + IAM role → GCS bucket name")
    name = "s3_iam"

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    cleanup_location(w, name)

    gcs_as_s3_url = f"s3://{BUCKET}"
    print(f"URL: {gcs_as_s3_url}")
    print(f"Credential: {EXISTING_CRED}")
    print("Strategy: Use existing IAM role credential with s3:// pointing at GCS bucket name")

    try:
        loc = w.external_locations.create(
            name=f"{LOC_PREFIX}_{name}",
            url=gcs_as_s3_url,
            credential_name=EXISTING_CRED,
            comment="Test: s3:// URL pointing at GCS bucket name",
            skip_validation=True,
        )
        print(f"Location created (skip_validation=True): {loc.name}")
        print(f"URL: {loc.url}")

        # Try to list files — this is where the real test happens
        print("Attempting to list files via UC...")
        try:
            result = spark.sql(f"LIST '{gcs_as_s3_url}/sample-data/'").collect()
            print(f"UNEXPECTED PASS: Listed {len(result)} files")
        except Exception as e:
            error_str = str(e)[:300]
            print(f"EXPECTED FAIL (list): {error_str}")
    except Exception as e:
        error_str = str(e)[:300]
        print(f"FAIL (create): {error_str}")
    finally:
        cleanup_location(w, name)


def test_r2_url_format_for_gcs(spark, dbutils):
    """Test 2: r2:// URL format but pointing at GCS endpoint.

    R2 URL format: r2://bucket@account-id.r2.cloudflarestorage.com
    Trick: r2://bucket@storage.googleapis.com

    Hypothesis: UC parses the r2:// URL and routes to the embedded endpoint.
    If so, we might be able to redirect to GCS. More likely, UC validates
    the endpoint domain.
    """
    print_header("Test 2: r2:// URL format → GCS endpoint")
    name = "r2_gcs"

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    cleanup_location(w, name)

    hmac_access_id = get_secret(dbutils, "hmac_access_id")
    hmac_secret = get_secret(dbutils, "hmac_secret")

    # Try creating R2-style credential with GCS HMAC keys
    r2_gcs_url = f"r2://{BUCKET}@storage.googleapis.com"
    print(f"URL: {r2_gcs_url}")
    print("Strategy: R2 credential type with GCS HMAC keys, r2:// URL pointing at GCS")

    try:
        # Create storage credential using R2 credential type with GCS HMAC keys
        # This uses the REST API approach since SQL doesn't support R2 credential creation
        from databricks.sdk.service.catalog import (
            CloudflareApiToken,
        )

        try:
            cred = w.storage_credentials.create(
                name=f"{CRED_PREFIX}_{name}",
                cloudflare_api_token=CloudflareApiToken(
                    access_key_id=hmac_access_id,
                    secret_access_key=hmac_secret,
                    account_id="gcs-fake-account",  # Required field — using fake value
                ),
                comment="Test: R2 credential type with GCS HMAC keys",
                skip_validation=True,
            )
            print(f"Storage credential created: {cred.name}")

            # Try creating external location with r2:// URL pointing at GCS
            try:
                loc = w.external_locations.create(
                    name=f"{LOC_PREFIX}_{name}",
                    url=r2_gcs_url,
                    credential_name=f"{CRED_PREFIX}_{name}",
                    comment="Test: r2:// URL redirected to GCS",
                    skip_validation=True,
                )
                print(f"UNEXPECTED PASS: External location created: {loc.name}")
                print(f"URL: {loc.url}")

                # Try to list files
                try:
                    result = spark.sql(f"LIST '{r2_gcs_url}/sample-data/'").collect()
                    print(f"UNEXPECTED PASS: Listed {len(result)} files")
                except Exception as e:
                    print(f"EXPECTED FAIL (list): {e}")
            except Exception as e:
                print(f"EXPECTED FAIL (create location): {e}")
        except Exception as e:
            print(f"EXPECTED FAIL (create credential): {e}")
    except ImportError as e:
        print(f"SKIP: Missing SDK types — {e}")
    finally:
        # Cleanup via SDK
        try:
            w.external_locations.delete(f"{LOC_PREFIX}_{name}", force=True)
        except Exception:
            pass
        try:
            w.storage_credentials.delete(f"{CRED_PREFIX}_{name}", force=True)
        except Exception:
            pass


def test_r2_credential_with_s3_url(spark, dbutils):
    """Test 3: R2 credential type (GCS HMAC keys) with plain s3:// URL.

    Hypothesis: Maybe UC will use the R2 credential's access keys with the
    S3 protocol but route to the standard S3 endpoint. The keys won't work
    against real S3, but if we could somehow override the endpoint...
    """
    print_header("Test 3: R2 credential + s3:// URL")
    name = "r2_cred_s3_url"

    hmac_access_id = get_secret(dbutils, "hmac_access_id")
    hmac_secret = get_secret(dbutils, "hmac_secret")

    s3_url = f"s3://{BUCKET}"
    print(f"URL: {s3_url}")
    print("Strategy: R2 credential (with GCS HMAC keys) + s3:// URL")

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import CloudflareApiToken

    w = WorkspaceClient()
    try:
        cred = w.storage_credentials.create(
            name=f"{CRED_PREFIX}_{name}",
            cloudflare_api_token=CloudflareApiToken(
                access_key_id=hmac_access_id,
                secret_access_key=hmac_secret,
                account_id="gcs-fake-account",
            ),
            comment="Test: R2 credential with GCS HMAC keys for s3:// URL",
            skip_validation=True,
        )
        print(f"Storage credential created: {cred.name}")

        try:
            loc = w.external_locations.create(
                name=f"{LOC_PREFIX}_{name}",
                url=s3_url,
                credential_name=f"{CRED_PREFIX}_{name}",
                comment="Test: R2 credential + s3:// URL",
                skip_validation=True,
            )
            print(f"UNEXPECTED PASS: External location created: {loc.name}")

            try:
                result = spark.sql(f"LIST '{s3_url}/sample-data/'").collect()
                print(f"UNEXPECTED PASS: Listed {len(result)} files")
            except Exception as e:
                print(f"EXPECTED FAIL (list): {e}")
        except Exception as e:
            print(f"EXPECTED FAIL (create location): {e}")
    except Exception as e:
        print(f"EXPECTED FAIL (create credential): {e}")
    finally:
        try:
            w.external_locations.delete(f"{LOC_PREFIX}_{name}", force=True)
        except Exception:
            pass
        try:
            w.storage_credentials.delete(f"{CRED_PREFIX}_{name}", force=True)
        except Exception:
            pass


def test_gs_url_on_aws(spark, dbutils):
    """Test 4: gs:// URL on AWS Databricks.

    Hypothesis: gs:// is only supported on GCP Databricks. On AWS, the URL
    scheme should be rejected at creation time.
    """
    print_header("Test 4: gs:// URL on AWS Databricks")
    name = "gs_on_aws"

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    cleanup_location(w, name)

    gs_url = f"gs://{BUCKET}"
    print(f"URL: {gs_url}")
    print(f"Credential: {EXISTING_CRED}")
    print("Strategy: Try gs:// URL directly on AWS workspace")

    try:
        loc = w.external_locations.create(
            name=f"{LOC_PREFIX}_{name}",
            url=gs_url,
            credential_name=EXISTING_CRED,
            comment="Test: gs:// URL on AWS Databricks",
            skip_validation=True,
        )
        print(f"UNEXPECTED PASS: External location created with gs:// on AWS: {loc.name}")
        print(f"URL: {loc.url}")

        # Try to list
        try:
            result = spark.sql(f"LIST '{gs_url}/sample-data/'").collect()
            print(f"UNEXPECTED PASS: Listed {len(result)} files")
        except Exception as e:
            error_str = str(e)[:300]
            print(f"EXPECTED FAIL (list): {error_str}")
    except Exception as e:
        error_str = str(e)[:300]
        print(f"EXPECTED FAIL: {error_str}")
    finally:
        cleanup_location(w, name)


def test_s3_url_embedded_endpoint(spark, dbutils):
    """Test 5: s3:// URL with endpoint embedded in various formats.

    Try creative URL formats that might trick UC's URL parser into using
    a different endpoint.
    """
    print_header("Test 5: Creative s3:// URL formats")
    name = "s3_creative"

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()

    # Try various URL formats
    urls = [
        f"s3://{BUCKET}.storage.googleapis.com",
        f"s3://storage.googleapis.com/{BUCKET}",
        f"s3a://{BUCKET}",
    ]

    for i, url in enumerate(urls):
        suffix = f"{name}_{i}"
        cleanup_location(w, suffix)
        print(f"\n  Trying URL: {url}")
        try:
            loc = w.external_locations.create(
                name=f"{LOC_PREFIX}_{suffix}",
                url=url,
                credential_name=EXISTING_CRED,
                comment=f"Test: creative URL format {i}",
                skip_validation=True,
            )
            print(f"  UNEXPECTED PASS: Created with URL: {loc.url}")

            # Try listing files if creation succeeded
            try:
                result = spark.sql(f"LIST '{url}/sample-data/'").collect()
                print(f"  UNEXPECTED PASS: Listed {len(result)} files")
            except Exception as e:
                error_str = str(e)[:200]
                print(f"  EXPECTED FAIL (list): {error_str}")
        except Exception as e:
            error_str = str(e)[:200]
            print(f"  EXPECTED FAIL: {error_str}")
        finally:
            cleanup_location(w, suffix)


def main():
    spark, dbutils = get_spark_and_dbutils()

    print("=" * 60)
    print("UC External Location GCS Access Tests")
    print("=" * 60)
    print()
    print("These tests attempt to create UC external locations that route")
    print("S3-compliant requests to GCS. Research predicts all will fail")
    print("because UC's credential types and URL schemes are hardcoded.")
    print()

    test_s3_url_with_iam_role(spark, dbutils)
    test_r2_url_format_for_gcs(spark, dbutils)
    test_r2_credential_with_s3_url(spark, dbutils)
    test_gs_url_on_aws(spark, dbutils)
    test_s3_url_embedded_endpoint(spark, dbutils)

    print_header("SUMMARY")
    print("See UC_EXTERNAL_LOCATION_RESEARCH.md for detailed analysis.")
    print("The non-UC Hadoop S3A approach (scripts 01-04) remains the")
    print("only viable path for GCS access from AWS Databricks.")

    print_summary(spark)


if __name__ == "__main__":
    main()
