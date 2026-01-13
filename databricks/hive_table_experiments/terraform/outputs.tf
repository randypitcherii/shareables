output "role_arn" {
  description = "ARN of the IAM role for Databricks storage credential (classic compute)"
  value       = aws_iam_role.databricks_glue.arn
}

# -----------------------------------------------------------------------------
# Serverless Instance Profile Outputs
# -----------------------------------------------------------------------------

output "serverless_role_arn" {
  description = "ARN of the IAM role for Databricks serverless compute"
  value       = aws_iam_role.databricks_serverless.arn
}

output "serverless_instance_profile_arn" {
  description = "ARN of the instance profile for Databricks serverless compute"
  value       = aws_iam_instance_profile.databricks_serverless.arn
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

# Helpful summary
output "next_steps" {
  description = "Instructions for next steps"
  value       = <<-EOT
    ✓ Terraform applied successfully!

    =============================================================================
    CLASSIC COMPUTE SETUP (IAM User Credentials)
    =============================================================================

    1. Copy the AWS credentials to use with Databricks:
       - Access Key ID: ${aws_iam_access_key.databricks_user.id}
       - Secret Access Key: (run `terraform output -raw aws_secret_access_key`)

    2. Use the Role ARN for Databricks storage credential:
       - ${aws_iam_role.databricks_glue.arn}

    =============================================================================
    SERVERLESS COMPUTE SETUP (Instance Profile)
    =============================================================================

    ** MANUAL STEPS REQUIRED IN DATABRICKS CONSOLE **

    1. Get your Databricks workspace ID from the URL:
       - URL format: https://<workspace>.cloud.databricks.com/?o=<WORKSPACE_ID>
       - The number after 'o=' is your workspace ID

    2. Update terraform.tfvars with the workspace ID:
       databricks_serverless_workspace_ids = ["databricks-serverless-<WORKSPACE_ID>"]

    3. Re-run terraform apply to update the trust policy

    4. Register the instance profile in Databricks:
       a. Go to Account Console > Cloud Resources > Instance Profiles
       b. Click "Add instance profile"
       c. Enter: ${aws_iam_instance_profile.databricks_serverless.arn}
       d. Enter Role ARN: ${aws_iam_role.databricks_serverless.arn}
       e. Click "Add"

    5. Configure SQL Warehouses to use the instance profile:
       a. In workspace, go to Settings > Compute > SQL warehouses
       b. Click "Manage" next to "SQL warehouses and serverless compute"
       c. Select the instance profile from the dropdown
       d. Click "Save"

    Note: Changing SQL warehouse settings restarts all running SQL warehouses.

    =============================================================================
    S3 BUCKET URIs
    =============================================================================

    - ${aws_s3_bucket.east_1a.bucket} → s3://${aws_s3_bucket.east_1a.bucket}/
    - ${aws_s3_bucket.east_1b.bucket} → s3://${aws_s3_bucket.east_1b.bucket}/
    - ${aws_s3_bucket.west_2.bucket} → s3://${aws_s3_bucket.west_2.bucket}/

    =============================================================================
    NEXT STEPS
    =============================================================================

    Deploy Databricks resources:
       make deploy-databricks
  EOT
}
