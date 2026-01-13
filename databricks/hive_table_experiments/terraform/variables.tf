variable "aws_profile" {
  description = "AWS CLI profile to use"
  type        = string
  default     = "your-aws-profile"
}

variable "aws_region_primary" {
  description = "Primary AWS region for most resources"
  type        = string
  default     = "us-east-1"
}

variable "aws_region_secondary" {
  description = "Secondary AWS region for cross-region tests"
  type        = string
  default     = "us-west-2"
}

variable "project_prefix" {
  description = "Prefix for all resource names"
  type        = string
  default     = "your-project-prefix"
}

variable "databricks_account_id" {
  description = "Databricks AWS account ID for IAM trust policy (the AWS account ID where Databricks control plane runs)"
  type        = string
  default     = "414351767826" # Databricks commercial regions AWS account ID
  # This is Databricks' AWS account ID, not your account ID
  # See: https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html
}

variable "databricks_uc_master_role_name" {
  description = "Name of the Databricks Unity Catalog master IAM role"
  type        = string
  default     = "unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
  # This is the UC master role that assumes your storage credential role
  # See: https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html
}

variable "databricks_workspace_id" {
  description = "Databricks workspace ID (from workspace URL) - optional, only needed for Unity Catalog storage credential"
  type        = string
  default     = ""
  # This should be extracted from the workspace URL or provided
  # Example: for dbc-1f943136-5e42.cloud.databricks.com, it would be "1f943136-5e42"
  # For Glue-as-hive_metastore setup with IAM user credentials, this is optional
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default = {
    Project     = "Hive Migration Evaluation"
    Environment = "dev"
    ManagedBy   = "Terraform"
  }
}

# -----------------------------------------------------------------------------
# Databricks Serverless Configuration
# -----------------------------------------------------------------------------

variable "databricks_serverless_account_id" {
  description = "AWS account ID for Databricks serverless compute (managed by Databricks)"
  type        = string
  default     = "790110701330" # Databricks commercial regions serverless account
  # Note: For GovCloud US, use "170655000336"
  # Note: For GovCloud US-DoD, use "170644377855"
}

variable "databricks_serverless_workspace_ids" {
  description = "List of Databricks workspace IDs that can use serverless compute with this role. Format: databricks-serverless-<workspace-id>"
  type        = list(string)
  default     = []
  # Example: ["databricks-serverless-1234567890123456", "databricks-serverless-9876543210987654"]
  # Get workspace ID from URL: https://<workspace>.cloud.databricks.com/?o=<workspace-id>
}

variable "glue_database_name" {
  description = "Name of the Glue catalog database for Hive tables"
  type        = string
  default     = "your_glue_database"
}
