output "aws_account_id" {
  value = local.account_id
}

output "aws_region" {
  value = var.aws_region
}

output "bucket" {
  description = "S3 bucket holding the Iceberg warehouse, UC metadata root, and Athena results"
  value       = aws_s3_bucket.warehouse.bucket
}

output "warehouse_path" {
  description = "Iceberg warehouse root both writers use"
  value       = "s3://${aws_s3_bucket.warehouse.bucket}/warehouse"
}

output "uc_metadata_root" {
  description = "storage_root for the foreign catalog (required for Iceberg reads)"
  value       = "s3://${aws_s3_bucket.warehouse.bucket}/uc-metadata"
}

output "glue_database" {
  value = aws_glue_catalog_database.eval.name
}

output "uc_federation_role_arn" {
  description = "Self-assuming IAM role for the UC service credential AND storage credential"
  value       = aws_iam_role.uc_federation.arn
}

output "athena_workgroup" {
  value = aws_athena_workgroup.eval.name
}

output "env_snippet" {
  description = "Paste into dev.env"
  value       = <<-EOT
    AWS_REGION=${var.aws_region}
    AWS_ACCOUNT_ID=${local.account_id}
    S3_BUCKET=${aws_s3_bucket.warehouse.bucket}
    GLUE_DATABASE=${aws_glue_catalog_database.eval.name}
    UC_FEDERATION_ROLE_ARN=${aws_iam_role.uc_federation.arn}
    ATHENA_WORKGROUP=${aws_athena_workgroup.eval.name}
  EOT
}
