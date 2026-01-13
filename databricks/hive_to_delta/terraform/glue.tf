# Glue database for Hive tables
resource "aws_glue_catalog_database" "glue_database" {
  name        = var.glue_database_name
  description = "Hive to Delta migration test database"
}

# Note: LakeFormation settings can be added in a separate lakeformation.tf
# if needed for permission management.
