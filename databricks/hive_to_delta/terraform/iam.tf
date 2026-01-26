# IAM Role for Databricks access to S3 and Glue
# Note: Self-assume trust policy is added separately to avoid circular dependency
resource "aws_iam_role" "databricks_glue" {
  name = "${var.project_prefix}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      # Allow Databricks Unity Catalog master role to assume this role
      {
        Sid    = "UCMasterRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${var.databricks_account_id}:role/${var.databricks_uc_master_role_name}"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.uc_external_id
          }
        }
      },
      # Allow current AWS account roles to assume (for testing)
      {
        Sid    = "LocalAssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action = "sts:AssumeRole"
      }
      ],
      # Conditionally add serverless compute trust policy if workspace IDs provided
      length(var.databricks_serverless_workspace_ids) > 0 ? [{
        Sid    = "ServerlessCompute"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${var.databricks_serverless_account_id}:role/serverless-customer-resource-role"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_serverless_workspace_ids
          }
        }
      }] : []
    )
  })

  tags = {
    Name = "${var.project_prefix}-role"
  }
}

# Get current AWS account ID
data "aws_caller_identity" "current" {}

# Instance profile for SQL Warehouse compute
# Wraps the IAM role so EC2 instances and serverless compute can assume it
resource "aws_iam_instance_profile" "databricks_htd" {
  name = "${var.project_prefix}-instance-profile"
  role = aws_iam_role.databricks_glue.name

  tags = {
    Name      = "${var.project_prefix}-instance-profile"
    ManagedBy = "terraform"
  }
}

# S3 access policy for all three buckets
resource "aws_iam_role_policy" "s3_access" {
  name = "${var.project_prefix}-s3-access"
  role = aws_iam_role.databricks_glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
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

# Glue Data Catalog full access
resource "aws_iam_role_policy" "glue_access" {
  name = "${var.project_prefix}-glue-access"
  role = aws_iam_role.databricks_glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:*"
        ]
        Resource = "*"
      }
    ]
  })
}

# LakeFormation permissions
resource "aws_iam_role_policy" "lakeformation_access" {
  name = "${var.project_prefix}-lakeformation-access"
  role = aws_iam_role.databricks_glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lakeformation:*"
        ]
        Resource = "*"
      }
    ]
  })
}

# Self-assume permission (required for UC SERVICE credentials)
resource "aws_iam_role_policy" "self_assume" {
  name = "${var.project_prefix}-self-assume"
  role = aws_iam_role.databricks_glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "sts:AssumeRole"
        Resource = aws_iam_role.databricks_glue.arn
      }
    ]
  })
}

# IAM User with programmatic credentials
resource "aws_iam_user" "databricks_user" {
  name = "${var.project_prefix}-user"

  tags = {
    Name = "${var.project_prefix}-user"
  }
}

# Policy allowing user to assume the role
resource "aws_iam_user_policy" "assume_role" {
  name = "${var.project_prefix}-assume-role"
  user = aws_iam_user.databricks_user.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "sts:AssumeRole"
        Resource = aws_iam_role.databricks_glue.arn
      }
    ]
  })
}

# Direct S3 and Glue access for the user (for running fixtures locally)
resource "aws_iam_user_policy" "direct_access" {
  name = "${var.project_prefix}-direct-access"
  user = aws_iam_user.databricks_user.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
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
      },
      {
        Effect   = "Allow"
        Action   = ["glue:*"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["lakeformation:*"]
        Resource = "*"
      }
    ]
  })
}

# Create access key for the user
resource "aws_iam_access_key" "databricks_user" {
  user = aws_iam_user.databricks_user.name
}
