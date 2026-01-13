"""Access method definitions and catalog routing for Glue table testing.

Defines three approaches for accessing Glue tables from Databricks:
1. glue_as_hive_metastore - Spark's built-in Glue integration (hive_metastore catalog)
2. uc_glue_federation - Unity Catalog foreign catalog pointing to Glue
3. uc_external_tables - Manual UC external tables (Parquet or Delta)

Each approach has different capabilities and limitations for partition discovery,
write operations, and Delta table features.
"""

import os
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class AccessMethod(Enum):
    """Three approaches for accessing Glue tables from Databricks."""

    GLUE_AS_HIVE_METASTORE = "glue_hive"
    UC_GLUE_FEDERATION = "glue_fed"
    UC_EXTERNAL_TABLES = "uc_ext"


class TableFormat(Enum):
    """Table format variants for UC external tables."""

    HIVE_PARQUET = "hive"  # Original Parquet format
    DELTA = "delta"  # Converted to Delta


@dataclass
class AccessMethodConfig:
    """Configuration and capabilities for each access method.

    Attributes:
        method: The access method enum value
        catalog: Catalog name to use in queries
        schema: Schema/database name
        table_suffix: Suffix to append to table names (e.g., "_glue_hive")
        s3_path_prefix: S3 path prefix for this method's tables
        requires_spark_config: Whether Spark config is needed (Glue hive_metastore)
        requires_service_credential: Whether a SERVICE credential is needed (federation)
        supports_external_partitions: Can query partitions outside table root
        supports_cross_bucket: Can query partitions across S3 buckets
        supports_write: Supports INSERT operations
        supports_update_delete: Supports UPDATE/DELETE (Delta only)
        supports_optimize: Supports OPTIMIZE command (Delta only)
        supports_vacuum: Supports VACUUM command (Delta only)
        data_copy_required: Whether data must be copied for multi-location scenarios
        reads_data_files: Whether setup/queries read actual S3 data files
        cold_storage_friendly: Works with S3 Glacier without restore
    """

    method: AccessMethod
    catalog: str
    schema: str
    table_suffix: str
    s3_path_prefix: str
    requires_spark_config: bool = False
    requires_service_credential: bool = False
    supports_external_partitions: bool = False
    supports_cross_bucket: bool = False
    supports_write: bool = False
    supports_update_delete: bool = False
    supports_optimize: bool = False
    supports_vacuum: bool = False
    data_copy_required: bool = False
    reads_data_files: bool = True
    cold_storage_friendly: bool = True
    table_format: Optional[TableFormat] = None

    def get_fully_qualified_table(self, table_base_name: str) -> str:
        """Generate fully-qualified table name for this access method.

        Args:
            table_base_name: Base table name (e.g., "standard")

        Returns:
            Full table path like "hive_metastore.my_database.standard_glue_hive"
        """
        table_name = f"{table_base_name}{self.table_suffix}"
        return f"{self.catalog}.{self.schema}.{table_name}"

    def get_table_name(self, table_base_name: str) -> str:
        """Generate table name with suffix.

        Args:
            table_base_name: Base table name (e.g., "standard")

        Returns:
            Table name like "standard_glue_hive"
        """
        return f"{table_base_name}{self.table_suffix}"

    def get_s3_table_path(self, bucket: str, table_base_name: str) -> str:
        """Generate S3 path for this access method's table.

        Args:
            bucket: S3 bucket name
            table_base_name: Base table name (e.g., "standard")

        Returns:
            S3 path like "s3://bucket/tables/glue_hive/standard/"
        """
        return f"s3://{bucket}/tables/{self.s3_path_prefix}/{table_base_name}/"


# Pre-configured access methods

GLUE_AS_HIVE = AccessMethodConfig(
    method=AccessMethod.GLUE_AS_HIVE_METASTORE,
    catalog="hive_metastore",
    schema=os.getenv("HIVE_EVAL_GLUE_DATABASE", "your_glue_database"),
    table_suffix="_glue_hive",
    s3_path_prefix="glue_hive",
    requires_spark_config=True,
    requires_service_credential=False,
    supports_external_partitions=True,  # KEY DISCOVERY: Works with Glue!
    supports_cross_bucket=True,
    supports_write=True,  # INSERT works
    supports_update_delete=False,  # External Hive tables don't support
    supports_optimize=False,  # Not applicable to Hive tables
    supports_vacuum=False,  # Not applicable to Hive tables
    data_copy_required=False,
    reads_data_files=True,
    cold_storage_friendly=True,  # Metadata-only registration
    table_format=TableFormat.HIVE_PARQUET,
)

UC_GLUE_FEDERATION = AccessMethodConfig(
    method=AccessMethod.UC_GLUE_FEDERATION,
    catalog=os.getenv("HIVE_EVAL_GLUE_CATALOG", "your_glue_catalog"),
    schema=os.getenv("HIVE_EVAL_GLUE_DATABASE", "your_glue_database"),
    table_suffix="_glue_fed",
    s3_path_prefix="glue_fed",
    requires_spark_config=False,
    requires_service_credential=True,  # SERVICE credential for Glue API
    supports_external_partitions=True,  # Federation inherits Glue behavior
    supports_cross_bucket=True,
    supports_write=False,  # Read-only access via federation
    supports_update_delete=False,
    supports_optimize=False,
    supports_vacuum=False,
    data_copy_required=False,
    reads_data_files=True,
    cold_storage_friendly=True,  # Metadata-only federation
    table_format=TableFormat.HIVE_PARQUET,
)

UC_EXTERNAL_HIVE = AccessMethodConfig(
    method=AccessMethod.UC_EXTERNAL_TABLES,
    catalog=os.getenv("HIVE_EVAL_UC_CATALOG", "your_catalog"),
    schema=os.getenv("HIVE_EVAL_UC_SCHEMA", "your_uc_schema"),
    table_suffix="_uc_ext",
    s3_path_prefix="uc_ext",
    requires_spark_config=False,
    requires_service_credential=False,  # Storage credential only
    supports_external_partitions=False,  # UC limitation!
    supports_cross_bucket=False,  # UC limitation!
    supports_write=True,  # INSERT works
    supports_update_delete=False,  # Parquet doesn't support
    supports_optimize=False,  # Not applicable to Parquet
    supports_vacuum=False,  # Not applicable to Parquet
    data_copy_required=True,  # For multi-location scenarios
    reads_data_files=True,
    cold_storage_friendly=True,
    table_format=TableFormat.HIVE_PARQUET,
)

UC_EXTERNAL_DELTA = AccessMethodConfig(
    method=AccessMethod.UC_EXTERNAL_TABLES,
    catalog=os.getenv("HIVE_EVAL_UC_CATALOG", "your_catalog"),
    schema=os.getenv("HIVE_EVAL_UC_SCHEMA", "your_uc_schema"),
    table_suffix="_uc_delta",
    s3_path_prefix="uc_delta",
    requires_spark_config=False,
    requires_service_credential=False,
    supports_external_partitions=False,  # Requires consolidation
    supports_cross_bucket=False,  # Requires consolidation
    supports_write=True,
    supports_update_delete=True,  # Delta supports ACID
    supports_optimize=True,  # Delta feature
    supports_vacuum=True,  # Delta feature
    data_copy_required=True,  # For multi-location scenarios
    reads_data_files=True,  # CONVERT TO DELTA reads files
    cold_storage_friendly=False,  # Need file access for conversion
    table_format=TableFormat.DELTA,
)

# All access method configs in order of testing
ALL_ACCESS_METHODS = [
    GLUE_AS_HIVE,
    UC_GLUE_FEDERATION,
    UC_EXTERNAL_HIVE,
    UC_EXTERNAL_DELTA,
]

# Mapping from suffix to config
ACCESS_METHOD_BY_SUFFIX = {
    "_glue_hive": GLUE_AS_HIVE,
    "_glue_fed": UC_GLUE_FEDERATION,
    "_uc_ext": UC_EXTERNAL_HIVE,
    "_uc_delta": UC_EXTERNAL_DELTA,
}


def get_access_method_config(suffix: str) -> Optional[AccessMethodConfig]:
    """Get access method config by table suffix.

    Args:
        suffix: Table suffix like "_glue_hive"

    Returns:
        AccessMethodConfig or None if not found
    """
    return ACCESS_METHOD_BY_SUFFIX.get(suffix)


def can_test_scenario_with_method(
    scenario_has_external_partitions: bool,
    scenario_crosses_buckets: bool,
    access_config: AccessMethodConfig,
) -> tuple[bool, str]:
    """Check if a scenario can be tested with an access method.

    Args:
        scenario_has_external_partitions: Whether scenario has partitions outside table root
        scenario_crosses_buckets: Whether scenario has partitions across S3 buckets
        access_config: The access method configuration

    Returns:
        Tuple of (can_test, reason_if_not)
    """
    if scenario_has_external_partitions and not access_config.supports_external_partitions:
        return False, "External partitions not supported by this access method"

    if scenario_crosses_buckets and not access_config.supports_cross_bucket:
        return False, "Cross-bucket partitions not supported by this access method"

    return True, "Compatible"
