# Hive to Delta Infrastructure

This directory contains Terraform configuration for deploying AWS infrastructure needed for the `hive_to_delta` package.

## Overview

This infrastructure creates:

- **3 S3 buckets** for multi-region testing:
  - `{project_prefix}-east-1a` (primary, us-east-1)
  - `{project_prefix}-east-1b` (secondary, us-east-1)
  - `{project_prefix}-west-2` (cross-region, us-west-2)
- **IAM role** for Databricks Unity Catalog access to S3 and Glue
- **IAM user** with access keys for local testing
- **Glue database** for Hive table metadata
- **Proper IAM permissions** for S3, Glue, and LakeFormation access

## Prerequisites

Before deploying this infrastructure, ensure you have:

1. **Terraform installed** (version >= 1.0)
   ```bash
   terraform version
   ```

2. **AWS CLI configured** with appropriate credentials
   ```bash
   aws configure
   # OR set environment variables:
   # export AWS_ACCESS_KEY_ID="your-access-key"
   # export AWS_SECRET_ACCESS_KEY="your-secret-key"
   # export AWS_DEFAULT_REGION="us-east-1"
   ```

3. **Required AWS permissions** to create:
   - S3 buckets
   - IAM roles and policies
   - IAM users and access keys
   - Glue databases

## Deployment Steps

### 1. Configure Variables

Copy the example variables file and customize it:

```bash
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your values:

```hcl
# Project prefix (will be used for all resource names)
project_prefix = "htd-myproject"

# Unity Catalog external ID (get from Databricks)
uc_external_id = "your-external-id-here"

# Glue database name
glue_database_name = "my_hive_database"
```

### 2. Initialize Terraform

```bash
terraform init
```

### 3. Review the Plan

```bash
terraform plan
```

Review the resources that will be created.

### 4. Apply the Configuration

```bash
terraform apply
```

Type `yes` to confirm and create the resources.

### 5. Retrieve Outputs

After successful deployment, retrieve important values:

```bash
# Get all outputs
terraform output

# Get specific sensitive outputs
terraform output -raw aws_access_key_id
terraform output -raw aws_secret_access_key

# Get role ARN for Databricks
terraform output -raw role_arn
```

## Configuration Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `aws_region_primary` | Primary AWS region | `us-east-1` |
| `aws_region_secondary` | Secondary AWS region for cross-region tests | `us-west-2` |
| `project_prefix` | Prefix for all resource names | `htd` |
| `databricks_account_id` | Databricks AWS account ID | `414351767826` |
| `databricks_uc_master_role_name` | UC master role name | `unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL` |
| `uc_external_id` | External ID for storage credential | Required |
| `glue_database_name` | Glue database name | `hive_to_delta_test` |

## Outputs

| Output | Description |
|--------|-------------|
| `role_arn` | IAM role ARN for Databricks storage credential |
| `bucket_east_1a` | Primary S3 bucket name |
| `bucket_east_1b` | Secondary S3 bucket name |
| `bucket_west_2` | West region S3 bucket name |
| `bucket_*_uri` | S3 URIs for each bucket |
| `glue_database_name` | Glue database name |
| `user_name` | IAM user name |
| `aws_access_key_id` | Access key ID (sensitive) |
| `aws_secret_access_key` | Secret access key (sensitive) |

## Databricks Integration

### Unity Catalog Storage Credential

1. Get the IAM role ARN:
   ```bash
   terraform output -raw role_arn
   ```

2. In Databricks, create a storage credential:
   ```bash
   databricks storage-credentials create htd_storage_credential \
     --aws-iam-role-arn "$(terraform output -raw role_arn)"
   ```

3. Create external locations for each bucket:
   ```bash
   databricks external-locations create htd_east_1a \
     --storage-credential htd_storage_credential \
     --url "$(terraform output -raw bucket_east_1a_uri)"
   ```

### Glue Catalog Connection

Configure Databricks to use the Glue catalog:

```sql
-- In Databricks SQL or notebook
CREATE CATALOG IF NOT EXISTS glue_catalog
USING CONNECTION glue_connection;

USE CATALOG glue_catalog;
SHOW DATABASES;
```

## Local Development

For local testing and development, use the IAM user credentials:

1. Get credentials:
   ```bash
   export AWS_ACCESS_KEY_ID=$(terraform output -raw aws_access_key_id)
   export AWS_SECRET_ACCESS_KEY=$(terraform output -raw aws_secret_access_key)
   export AWS_DEFAULT_REGION="us-east-1"
   ```

2. Run tests:
   ```bash
   cd ..
   make test
   ```

## Multi-Region Setup

This infrastructure deploys S3 buckets across multiple regions to test cross-region scenarios:

- **us-east-1**: Two buckets (east-1a, east-1b) for same-region tests
- **us-west-2**: One bucket for cross-region tests

This allows testing of:
- Cross-bucket migrations within the same region
- Cross-region migrations
- Performance comparisons

## Security Best Practices

All S3 buckets have:
- Public access blocked
- Versioning enabled
- Encryption at rest (AWS managed keys)

IAM roles follow least privilege:
- Databricks role has only necessary S3, Glue, and LakeFormation permissions
- IAM user has limited scope for testing only

## Cleanup

To destroy all created resources:

```bash
terraform destroy
```

**Warning**: This will delete all S3 buckets and their contents, IAM roles, and the Glue database.

## Troubleshooting

### Permission Denied Errors

If you encounter permission errors:

1. Verify your AWS credentials have sufficient permissions
2. Check the trust policy allows Databricks to assume the role
3. Verify the external ID matches between Terraform and Databricks

### S3 Bucket Already Exists

If bucket names conflict:

1. Change `project_prefix` in `terraform.tfvars`
2. Run `terraform plan` to verify new names
3. Run `terraform apply`

### External ID Mismatch

If Databricks cannot assume the role:

1. Get the external ID from Databricks:
   ```bash
   databricks storage-credentials describe htd_storage_credential
   ```

2. Update `uc_external_id` in `terraform.tfvars`

3. Apply changes:
   ```bash
   terraform apply
   ```

## Support

For issues specific to this infrastructure, check:
- [AWS Terraform Provider Documentation](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [Databricks Unity Catalog Documentation](https://docs.databricks.com/en/connect/unity-catalog/)
- Project repository issues
