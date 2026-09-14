variable "aws_region" {
  description = "AWS region for the Glue database, S3 bucket, and Athena workgroup. The Glue Iceberg REST endpoint is regional: https://glue.<region>.amazonaws.com/iceberg"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "AWS CLI profile (SSO) used by Terraform and by the writer scripts"
  type        = string
}

variable "name_prefix" {
  description = "Prefix for every resource name. Keep it short; it is embedded in the bucket name."
  type        = string
  default     = "glue-iceberg-list-eval"
}

variable "glue_database" {
  description = "Glue database that both writer paths register tables into"
  type        = string
  default     = "glue_iceberg_list_eval"
}

variable "databricks_account_id" {
  description = <<-EOT
    Databricks account ID (account console -> top-right menu). Used as the
    sts:ExternalId condition on the Unity Catalog trust statement, exactly as the
    service-credential and storage-credential setup docs require.
  EOT
  type        = string
}

variable "databricks_uc_master_role_arn" {
  description = <<-EOT
    The Unity Catalog master IAM role Databricks assumes into your account.
    The docs publish this value; it is the same ARN the storage-credential
    CloudFormation template trusts. Override only if your deployment differs.
  EOT
  type        = string
  default     = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
}

variable "tags" {
  description = "Tags applied to all resources"
  type        = map(string)
  default = {
    project   = "glue-iceberg-list-type-uc-federation"
    ephemeral = "true"
  }
}
