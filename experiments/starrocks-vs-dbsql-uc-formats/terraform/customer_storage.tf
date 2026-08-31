# Optional "customer-managed storage" rig: an S3 bucket + IAM role shaped for a
# Unity Catalog storage credential, so the StarRocks-write battery can also run
# against a schema whose managed storage the evaluator owns (no managed-storage
# service control policies in the path).
#
# Two-phase: first apply creates the role trusting the UC master role with a
# placeholder external id; after `databricks storage-credentials create`
# returns the real external id, re-apply with -var uc_external_id=<id>.

variable "enable_customer_storage" {
  description = "Create the customer-managed storage bucket + UC role"
  type        = bool
  default     = false
}

variable "uc_master_role_arn" {
  description = "Databricks Unity Catalog master role (AWS commercial)"
  type        = string
  default     = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
}

variable "uc_external_id" {
  description = "External id from the created storage credential (placeholder on first apply)"
  type        = string
  default     = "placeholder"
}

data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "uc_eval" {
  count         = var.enable_customer_storage ? 1 : 0
  bucket        = "starrocks-uc-eval-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
  tags          = var.tags
}

locals {
  uc_role_name = "starrocks-uc-eval-uc-access"
  uc_role_arn  = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.uc_role_name}"
}

resource "aws_iam_role" "uc_access" {
  count = var.enable_customer_storage ? 1 : 0
  name  = local.uc_role_name
  tags  = var.tags

  # UC master role + self-assumption (required by UC validation).
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = var.uc_master_role_arn }
        Action    = "sts:AssumeRole"
        Condition = { StringEquals = { "sts:ExternalId" = var.uc_external_id } }
      },
      {
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "sts:AssumeRole"
        Condition = { ArnEquals = { "aws:PrincipalArn" = local.uc_role_arn } }
      }
    ]
  })
}

resource "aws_iam_role_policy" "uc_access" {
  count = var.enable_customer_storage ? 1 : 0
  name  = "uc-bucket-access"
  role  = aws_iam_role.uc_access[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:ListBucket", "s3:GetBucketLocation",
          "s3:ListBucketMultipartUploads", "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload"
        ]
        Resource = [
          aws_s3_bucket.uc_eval[0].arn,
          "${aws_s3_bucket.uc_eval[0].arn}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["sts:AssumeRole"]
        Resource = [local.uc_role_arn]
      }
    ]
  })
}

output "customer_storage_bucket" {
  value = var.enable_customer_storage ? aws_s3_bucket.uc_eval[0].bucket : null
}

output "uc_access_role_arn" {
  value = var.enable_customer_storage ? aws_iam_role.uc_access[0].arn : null
}
