output "role_arn" {
  description = "ARN of the IAM role for Databricks storage credential"
  value       = aws_iam_role.databricks_glue.arn
}

output "bucket_east_1a" {
  description = "Name of the primary S3 bucket in us-east-1"
  value       = aws_s3_bucket.east_1a.bucket
}

output "bucket_east_1b" {
  description = "Name of the secondary S3 bucket in us-east-1"
  value       = aws_s3_bucket.east_1b.bucket
}

output "bucket_west_2" {
  description = "Name of the S3 bucket in us-west-2"
  value       = aws_s3_bucket.west_2.bucket
}

output "bucket_east_1a_uri" {
  description = "S3 URI of the primary bucket"
  value       = "s3://${aws_s3_bucket.east_1a.bucket}"
}

output "bucket_east_1b_uri" {
  description = "S3 URI of the secondary bucket"
  value       = "s3://${aws_s3_bucket.east_1b.bucket}"
}

output "bucket_west_2_uri" {
  description = "S3 URI of the west-2 bucket"
  value       = "s3://${aws_s3_bucket.west_2.bucket}"
}

output "glue_database_name" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.glue_database.name
}

output "user_name" {
  description = "IAM user name"
  value       = aws_iam_user.databricks_user.name
}

output "aws_access_key_id" {
  description = "AWS access key ID for the IAM user"
  value       = aws_iam_access_key.databricks_user.id
  sensitive   = true
}

output "aws_secret_access_key" {
  description = "AWS secret access key for the IAM user"
  value       = aws_iam_access_key.databricks_user.secret
  sensitive   = true
}

output "next_steps" {
  description = "Instructions for next steps"
  value       = <<-EOT
    Terraform applied successfully!

    =============================================================================
    UNITY CATALOG SETUP
    =============================================================================

    1. Use the Role ARN for Databricks storage credential:
       - ${aws_iam_role.databricks_glue.arn}

    2. Copy the AWS credentials for local testing:
       - Access Key ID: ${aws_iam_access_key.databricks_user.id}
       - Secret Access Key: (run `terraform output -raw aws_secret_access_key`)

    =============================================================================
    S3 BUCKET URIs
    =============================================================================

    - ${aws_s3_bucket.east_1a.bucket} → s3://${aws_s3_bucket.east_1a.bucket}/
    - ${aws_s3_bucket.east_1b.bucket} → s3://${aws_s3_bucket.east_1b.bucket}/
    - ${aws_s3_bucket.west_2.bucket} → s3://${aws_s3_bucket.west_2.bucket}/

    =============================================================================
    GLUE DATABASE
    =============================================================================

    - Database: ${aws_glue_catalog_database.glue_database.name}

    =============================================================================
    NEXT STEPS
    =============================================================================

    1. Configure Unity Catalog storage credential in Databricks
    2. Create external location pointing to S3 buckets
    3. Set up Glue catalog connection
    4. Run conversion tests with hive_to_delta package
  EOT
}
