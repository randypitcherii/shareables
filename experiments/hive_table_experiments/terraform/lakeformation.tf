# =============================================================================
# AWS Lake Formation Configuration
# =============================================================================
# Configures LakeFormation data location registrations and permissions for
# Databricks serverless compute to access Glue tables and S3 data.
#
# Note: The project currently uses IAM-only mode (IAM_ALLOWED_PRINCIPALS)
# configured in glue.tf. This file provides the additional LakeFormation
# resources needed if/when LakeFormation permission mode is enabled.
#
# References:
# - https://docs.aws.amazon.com/lake-formation/latest/dg/granting-table-permissions.html
# - https://docs.aws.amazon.com/lake-formation/latest/dg/lf-permissions-reference.html
# =============================================================================

# -----------------------------------------------------------------------------
# Data Lake Settings
# -----------------------------------------------------------------------------
# Configure LakeFormation with both classic and serverless roles as administrators.
# Uses IAM-only access control mode (IAM_ALLOWED_PRINCIPALS) which allows
# IAM policies to work without additional LakeFormation permission grants.
# -----------------------------------------------------------------------------

resource "aws_lakeformation_data_lake_settings" "default" {
  admins = [
    aws_iam_role.databricks_glue.arn,
    aws_iam_role.databricks_serverless.arn,
    aws_iam_user.databricks_user.arn
  ]

  # Use IAM access control mode (not LakeFormation-only mode)
  # This allows IAM policies to work without additional LakeFormation permissions
  create_database_default_permissions {
    principal   = "IAM_ALLOWED_PRINCIPALS"
    permissions = ["ALL"]
  }

  create_table_default_permissions {
    principal   = "IAM_ALLOWED_PRINCIPALS"
    permissions = ["ALL"]
  }
}

# -----------------------------------------------------------------------------
# Data Location Registration
# -----------------------------------------------------------------------------
# NOTE: Commenting out Lake Formation resource registration so IAM-only
# permissions work for table creation. When locations are registered with
# LakeFormation, DATA_LOCATION_ACCESS grants are required to create tables
# pointing to those locations.
#
# For serverless compute, the IAM instance profile provides S3 access directly.
# -----------------------------------------------------------------------------

# resource "aws_lakeformation_resource" "east_1a" {
#   arn      = aws_s3_bucket.east_1a.arn
#   role_arn = aws_iam_role.databricks_serverless.arn
#   use_service_linked_role = false
# }

# resource "aws_lakeformation_resource" "east_1b" {
#   arn      = aws_s3_bucket.east_1b.arn
#   role_arn = aws_iam_role.databricks_serverless.arn
#   use_service_linked_role = false
# }

# Note: West-2 bucket registration would need to be done in the us-west-2 region
# LakeFormation is regional, so cross-region buckets need separate configuration
# For now, we rely on IAM policies for west-2 bucket access

# -----------------------------------------------------------------------------
# Database Permissions for Serverless Role
# -----------------------------------------------------------------------------
# Grants the serverless role permissions on the Glue database.
# Even with IAM_ALLOWED_PRINCIPALS, explicit grants may be needed for
# some operations when LakeFormation is enabled.
# -----------------------------------------------------------------------------

resource "aws_lakeformation_permissions" "serverless_database" {
  principal   = aws_iam_role.databricks_serverless.arn
  permissions = ["ALL", "ALTER", "CREATE_TABLE", "DESCRIBE", "DROP"]

  database {
    name = aws_glue_catalog_database.glue_database.name
  }

  # Depends on the role being a LakeFormation admin
  depends_on = [aws_lakeformation_data_lake_settings.default]
}

# NOTE: Table and data location permission grants are commented out because
# they fail when Lake Formation is in IAM_ALLOWED_PRINCIPALS mode. The IAM
# policies provide the necessary permissions instead.

# resource "aws_lakeformation_permissions" "serverless_tables" {
#   principal   = aws_iam_role.databricks_serverless.arn
#   permissions = ["ALL", "ALTER", "DELETE", "DESCRIBE", "DROP", "INSERT", "SELECT"]
#   table {
#     database_name = aws_glue_catalog_database.glue_database.name
#     wildcard      = true
#   }
#   depends_on = [aws_lakeformation_data_lake_settings.default]
# }

# resource "aws_lakeformation_permissions" "serverless_location_east_1a" {
#   principal   = aws_iam_role.databricks_serverless.arn
#   permissions = ["DATA_LOCATION_ACCESS"]
#   data_location {
#     arn = aws_s3_bucket.east_1a.arn
#   }
#   depends_on = [aws_lakeformation_resource.east_1a]
# }

# resource "aws_lakeformation_permissions" "serverless_location_east_1b" {
#   principal   = aws_iam_role.databricks_serverless.arn
#   permissions = ["DATA_LOCATION_ACCESS"]
#   data_location {
#     arn = aws_s3_bucket.east_1b.arn
#   }
#   depends_on = [aws_lakeformation_resource.east_1b]
# }

# resource "aws_lakeformation_permissions" "classic_location_east_1a" {
#   principal   = aws_iam_role.databricks_glue.arn
#   permissions = ["DATA_LOCATION_ACCESS"]
#   data_location {
#     arn = aws_s3_bucket.east_1a.arn
#   }
#   depends_on = [aws_lakeformation_resource.east_1a]
# }

# resource "aws_lakeformation_permissions" "classic_location_east_1b" {
#   principal   = aws_iam_role.databricks_glue.arn
#   permissions = ["DATA_LOCATION_ACCESS"]
#   data_location {
#     arn = aws_s3_bucket.east_1b.arn
#   }
#   depends_on = [aws_lakeformation_resource.east_1b]
# }

# resource "aws_lakeformation_permissions" "user_database" {
#   principal   = aws_iam_user.databricks_user.arn
#   permissions = ["ALL", "ALTER", "CREATE_TABLE", "DESCRIBE", "DROP"]
#   database {
#     name = aws_glue_catalog_database.glue_database.name
#   }
#   depends_on = [aws_lakeformation_data_lake_settings.default]
# }

# resource "aws_lakeformation_permissions" "user_tables" {
#   principal   = aws_iam_user.databricks_user.arn
#   permissions = ["ALL", "ALTER", "DELETE", "DESCRIBE", "DROP", "INSERT", "SELECT"]
#   table {
#     database_name = aws_glue_catalog_database.glue_database.name
#     wildcard      = true
#   }
#   depends_on = [aws_lakeformation_data_lake_settings.default]
# }

# resource "aws_lakeformation_permissions" "user_location_east_1a" {
#   principal   = aws_iam_user.databricks_user.arn
#   permissions = ["DATA_LOCATION_ACCESS"]
#   data_location {
#     arn = aws_s3_bucket.east_1a.arn
#   }
#   depends_on = [aws_lakeformation_resource.east_1a]
# }

# resource "aws_lakeformation_permissions" "user_location_east_1b" {
#   principal   = aws_iam_user.databricks_user.arn
#   permissions = ["DATA_LOCATION_ACCESS"]
#   data_location {
#     arn = aws_s3_bucket.east_1b.arn
#   }
#   depends_on = [aws_lakeformation_resource.east_1b]
# }
