# Integration Test Results - Final

## Summary

**All tests passing!**

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| PASSED | 19 | 76 | +57 (400% improvement) |
| FAILED | 2 | 0 | Fixed! |
| SKIPPED | 18 | 18 | No change |
| **Total** | **19/94** | **76/94** | **81% pass rate** |

## What Was Fixed

### Issue #1: SQL Warehouse Instance Profile Configuration
**Symptom:** SQL Warehouses couldn't start due to IAM misconfiguration
**Root Cause:** Missing instance profile and serverless compute trust policy
**Fix:**
1. Created IAM instance profile (`htd-instance-profile`)
2. Added serverless compute trust policy to IAM role
3. Configured workspace with instance profile via CLI

### Issue #2: Unity Catalog External ID Mismatch
**Symptom:** `PERMISSION_DENIED: Access denied. credentialName = None`
**Root Cause:** IAM trust policy had placeholder external ID (`test-external-id`) instead of the actual Unity Catalog external ID (get this from your storage credential)
**Fix:** Updated `terraform/terraform.tfvars` with correct external ID and re-applied terraform

## Infrastructure Configuration

### AWS (via Terraform)
- IAM Role: `htd-role` with S3/Glue/LakeFormation permissions
- IAM Instance Profile: `htd-instance-profile`
- Trust Policy Statements:
  1. **UC Master Role** - External ID: `<UC_EXTERNAL_ID>` (from storage credential)
  2. **Local Assume Role** - For local testing
  3. **Serverless Compute** - External ID: `databricks-serverless-<WORKSPACE_ID>`
- S3 Buckets: htd-east-1a, htd-east-1b, htd-west-2

### Databricks
- Storage Credential: `htd_storage_credential` -> `htd-role`
- External Locations: htd_east_1a, htd_east_1b, htd_west_2
- Workspace Config: Instance profile assigned for SQL Warehouses + serverless compute
- Serverless SQL Warehouse: RUNNING + HEALTHY

## Test Execution

```bash
export HIVE_TO_DELTA_TEST_WAREHOUSE_ID=<your-warehouse-id>
make test
```

**Duration:** ~2 minutes

## Test Results Breakdown

### PASSED (76 tests)
- Schema validation tests
- Conversion tests (standard, cross-bucket, cross-region)
- SQL Warehouse query tests
- Vacuum tests (standard, external, retention)
- Shallow clone tests (standard, cross-bucket, queryable)
- Delta log utility tests

### SKIPPED (18 tests)
- Bulk conversion tests (marked as skip for CI efficiency)
- Pattern matching tests (marked as skip)
- Operations tests (SELECT, INSERT, UPDATE, OPTIMIZE, VACUUM via warehouse - marked as skip)

## Key Technical Insights

### External ID in Unity Catalog
Unity Catalog uses an external ID as a "password" when assuming IAM roles:
1. When you create a storage credential, Unity Catalog generates a unique external ID
2. Your IAM role's trust policy must include this exact external ID
3. Without matching external IDs, AssumeRole fails with 403 Forbidden

### Serverless Compute Trust Policy
Serverless SQL Warehouses and Databricks Connect serverless require a separate trust policy:
- Principal: `arn:aws:iam::790110701330:role/serverless-customer-resource-role`
- External ID format: `databricks-serverless-<WORKSPACE_ID>`

### Instance Profile vs Storage Credential
- **Instance Profile**: Used by SQL Warehouses and serverless compute for S3 access
- **Storage Credential**: Used by Unity Catalog for external location credential resolution
- Both must be properly configured for full functionality

## Files Modified

| File | Change |
|------|--------|
| `terraform/iam.tf` | Added instance profile + serverless trust policy |
| `terraform/outputs.tf` | Added instance profile outputs |
| `terraform/terraform.tfvars` | Fixed UC external ID (from storage credential) |

## Git Commits

```
ba78544 Update test results with IAM configuration findings
114c3a1 Fix SqlWarehouseConnection to support databricks-cli auth
797a8dd Add comprehensive test results documentation
```
