# =============================================================================
# Serverless Instance Profile for Databricks
# =============================================================================
# This creates an IAM role specifically for Databricks serverless compute
# (SQL warehouses, notebooks, jobs, DLT pipelines) to access S3 and Glue.
#
# References:
# - https://docs.databricks.com/aws/en/connect/storage/tutorial-s3-instance-profile
# - https://docs.databricks.com/aws/en/admin/sql/data-access-configuration
# =============================================================================

# -----------------------------------------------------------------------------
# IAM Role for Databricks Serverless Compute
# -----------------------------------------------------------------------------
# This role allows Databricks serverless compute resources to assume it
# via the serverless-customer-resource-role managed by Databricks.
# -----------------------------------------------------------------------------

resource "aws_iam_role" "databricks_serverless" {
  name = "${var.project_prefix}-serverless-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Self-assumption: Required for Databricks storage credentials
      # Databricks blocks credentials based on IAM roles that are not self-assuming
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action = "sts:AssumeRole"
      },
      # Databricks Serverless Compute trust relationship
      # This allows serverless SQL warehouses and serverless compute to assume this role
      # Principal is the Databricks-managed serverless-customer-resource-role
      {
        Effect = "Allow"
        Principal = {
          AWS = ["arn:aws:iam::${var.databricks_serverless_account_id}:role/serverless-customer-resource-role"]
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_serverless_workspace_ids
          }
        }
      }
    ]
  })

  tags = {
    Name = "${var.project_prefix}-serverless-role"
  }
}

# -----------------------------------------------------------------------------
# IAM Instance Profile
# -----------------------------------------------------------------------------
# Instance profiles wrap IAM roles for use with EC2/Databricks compute.
# For serverless, the role ARN is what matters, but we create the instance
# profile for completeness and potential classic compute use.
# -----------------------------------------------------------------------------

resource "aws_iam_instance_profile" "databricks_serverless" {
  name = "${var.project_prefix}-serverless-profile"
  role = aws_iam_role.databricks_serverless.name

  tags = {
    Name = "${var.project_prefix}-serverless-profile"
  }
}

# -----------------------------------------------------------------------------
# S3 Access Policy
# -----------------------------------------------------------------------------
# Grants read/write access to all three project S3 buckets.
# Required permissions for Delta Lake operations:
# - GetObject, GetObjectVersion: Read data
# - PutObject: Write data
# - DeleteObject: Delta maintenance (vacuum, compaction)
# - ListBucket: List objects in bucket
# - GetBucketLocation: Required for regional bucket access
# -----------------------------------------------------------------------------

resource "aws_iam_role_policy" "serverless_s3_access" {
  name = "${var.project_prefix}-serverless-s3-access"
  role = aws_iam_role.databricks_serverless.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3BucketAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.east_1a.arn,
          "${aws_s3_bucket.east_1a.arn}/*",
          aws_s3_bucket.east_1b.arn,
          "${aws_s3_bucket.east_1b.arn}/*",
          aws_s3_bucket.west_2.arn,
          "${aws_s3_bucket.west_2.arn}/*"
        ]
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# Glue Data Catalog Access Policy
# -----------------------------------------------------------------------------
# Grants full access to the Glue Data Catalog for metadata operations.
# This allows creating, reading, and managing databases and tables.
# -----------------------------------------------------------------------------

resource "aws_iam_role_policy" "serverless_glue_access" {
  name = "${var.project_prefix}-serverless-glue-access"
  role = aws_iam_role.databricks_serverless.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetTableVersion",
          "glue:GetTableVersions",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchDeletePartition"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region_primary}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.aws_region_primary}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.glue_database.name}",
          "arn:aws:glue:${var.aws_region_primary}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.glue_database.name}/*"
        ]
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# LakeFormation Access Policy
# -----------------------------------------------------------------------------
# Grants LakeFormation GetDataAccess permission required when LakeFormation
# is managing permissions on the data locations.
# -----------------------------------------------------------------------------

resource "aws_iam_role_policy" "serverless_lakeformation_access" {
  name = "${var.project_prefix}-serverless-lakeformation-access"
  role = aws_iam_role.databricks_serverless.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "LakeFormationDataAccess"
        Effect = "Allow"
        Action = [
          "lakeformation:GetDataAccess"
        ]
        Resource = "*"
      }
    ]
  })
}
