# Glue database for Hive tables
resource "aws_glue_catalog_database" "glue_database" {
  name        = var.glue_database_name
  description = "Hive compatibility validation test database"
}

# Note: LakeFormation settings moved to lakeformation.tf to consolidate
# all LakeFormation configuration in one place.
