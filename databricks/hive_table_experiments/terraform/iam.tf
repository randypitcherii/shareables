# IAM Role for Databricks access to S3 and Glue
resource "aws_iam_role" "databricks_glue" {
  name = "${var.project_prefix}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Allow Databricks Unity Catalog master role to assume this role with STORAGE credential
      {
        Sid    = "UCMasterRoleStorageCredential"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${var.databricks_account_id}:role/${var.databricks_uc_master_role_name}"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = "YOUR_EXTERNAL_ID_HERE"
          }
        }
      },
      # Allow Databricks Unity Catalog master role to assume this role with SERVICE credential
      {
        Sid    = "UCMasterRoleServiceCredential"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${var.databricks_account_id}:role/${var.databricks_uc_master_role_name}"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = "YOUR_EXTERNAL_ID_HERE"
          }
        }
      },
      # Self-assume (required for UC credential validation)
      {
        Sid    = "SelfAssume"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.project_prefix}-role"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = [
              "YOUR_EXTERNAL_ID_HERE",
              "YOUR_EXTERNAL_ID_HERE"
            ]
          }
        }
      }
    ]
  })

  tags = {
    Name = "${var.project_prefix}-role"
  }
}

# Get current AWS account ID
data "aws_caller_identity" "current" {}

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
