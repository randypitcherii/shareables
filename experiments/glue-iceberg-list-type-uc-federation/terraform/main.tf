terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

data "aws_caller_identity" "current" {}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  bucket_name = "${var.name_prefix}-${local.account_id}-${var.aws_region}"
  role_name   = "${var.name_prefix}-uc-federation"
  role_arn    = "arn:aws:iam::${local.account_id}:role/${local.role_name}"
}

# ---------------------------------------------------------------------------
# S3: one bucket holds the Iceberg warehouse (data + metadata.json written by
# the Iceberg writers), the foreign catalog's UC metadata storage_root, and the
# Athena query-results prefix. Everything is ephemeral; force_destroy on.
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "warehouse" {
  bucket        = local.bucket_name
  force_destroy = true
  tags          = var.tags
}

resource "aws_s3_bucket_public_access_block" "warehouse" {
  bucket                  = aws_s3_bucket.warehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# Glue: a single database. Both writer paths (Glue Iceberg REST endpoint and
# the Iceberg-native GlueCatalog) register their tables here, so the only
# variable between them is *how the StorageDescriptor was written*.
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "eval" {
  name         = var.glue_database
  description  = "Iceberg list-type UC federation bounds experiment (ephemeral)"
  location_uri = "s3://${aws_s3_bucket.warehouse.bucket}/warehouse/${var.glue_database}"
  tags         = var.tags
}

# ---------------------------------------------------------------------------
# IAM: ONE self-assuming role that serves as both the Unity Catalog *service
# credential* (Glue API access, used by the Glue connection) and the *storage
# credential* (S3 access, used by the external location the foreign catalog
# reads through). Databricks requires the role to be self-assuming; the trust
# policy therefore names both the UC master role and this role's own ARN.
#
# The self-reference has to be by ARN string (not the resource attribute) or
# Terraform reports a cycle. The role is created first, then the trust policy
# is attached in a second step so the self-assume principal already exists.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "trust" {
  statement {
    sid     = "DatabricksUnityCatalog"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.databricks_uc_master_role_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_account_id]
    }
  }

  statement {
    sid     = "SelfAssume"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [local.role_arn]
    }
  }
}

resource "aws_iam_role" "uc_federation" {
  name               = local.role_name
  assume_role_policy = data.aws_iam_policy_document.trust.json
  tags               = var.tags
}

data "aws_iam_policy_document" "glue_read" {
  statement {
    sid    = "GlueReadOnlyCatalog"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetTableVersion",
      "glue:GetTableVersions",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
      "glue:GetUserDefinedFunctions",
      "glue:GetCatalog",
      "glue:SearchTables",
    ]
    resources = ["*"]
  }

  statement {
    sid       = "StsSelfAssume"
    effect    = "Allow"
    actions   = ["sts:AssumeRole"]
    resources = [local.role_arn]
  }
}

data "aws_iam_policy_document" "s3_access" {
  statement {
    sid    = "BucketAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      aws_s3_bucket.warehouse.arn,
      "${aws_s3_bucket.warehouse.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_read" {
  name   = "glue-read"
  role   = aws_iam_role.uc_federation.id
  policy = data.aws_iam_policy_document.glue_read.json
}

resource "aws_iam_role_policy" "s3_access" {
  name   = "s3-access"
  role   = aws_iam_role.uc_federation.id
  policy = data.aws_iam_policy_document.s3_access.json
}

# ---------------------------------------------------------------------------
# Athena: a workgroup for the cross-reader row (does a *non-Databricks* Glue
# reader choke on the same `list<...>` StorageDescriptor type?). Results go to
# a prefix in the same bucket.
# ---------------------------------------------------------------------------

resource "aws_athena_workgroup" "eval" {
  name          = "${var.name_prefix}-athena"
  force_destroy = true
  tags          = var.tags

  configuration {
    enforce_workgroup_configuration = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.warehouse.bucket}/athena-results/"
    }
  }
}
