"""Hive table scenario definitions.

Defines test scenarios for evaluating Glue table access methods in Databricks.
Each scenario represents a different partition layout challenge:
- Basic layouts: standard partitioning, nested directories
- External partitions: partitions outside table root
- Multi-location: cross-bucket, cross-region, shared partitions
"""

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional

# =============================================================================
# S3 Bucket Configuration
# =============================================================================
# These buckets are used for Hive table scenarios. Override via environment
# variables when deploying to your own AWS environment.
#
# BUCKET_EAST_1A: Primary bucket in us-east-1 (most scenarios use this)
# BUCKET_EAST_1B: Secondary bucket in us-east-1 (for cross-bucket scenarios)
# BUCKET_WEST_2:  Bucket in us-west-2 (for cross-region scenarios)
# =============================================================================
BUCKET_EAST_1A = os.getenv("HIVE_EVAL_BUCKET_EAST_1A", "your-bucket-east-1a")
BUCKET_EAST_1B = os.getenv("HIVE_EVAL_BUCKET_EAST_1B", "your-bucket-east-1b")
BUCKET_WEST_2 = os.getenv("HIVE_EVAL_BUCKET_WEST_2", "your-bucket-west-2")

if TYPE_CHECKING:
    from .access_methods import AccessMethodConfig


@dataclass
class PartitionLocation:
    """Represents a partition and its S3 location.

    Attributes:
        partition_key: The partition column name (e.g., "region")
        partition_value: The partition value (e.g., "us-east")
        s3_path: Full S3 path to the partition data
    """

    partition_key: str
    partition_value: str
    s3_path: str


@dataclass
class HiveTableScenario:
    """Defines a Hive table test scenario.

    Attributes:
        name: Full scenario name (e.g., "standard_table")
        description: Human-readable description
        table_location: S3 path for the table root
        partitions: List of partition locations
        use_external_partitions: Whether partitions exist outside table root
        recursive_scan_required: Whether recursive file discovery is needed
        capability_group: Grouping for related scenarios
        crosses_buckets: Whether partitions span multiple S3 buckets
        crosses_regions: Whether partitions span AWS regions
        shares_partitions: Whether partitions are shared with other tables
        base_name: Short name for table suffixing (derived from name)
    """

    name: str
    description: str
    table_location: str
    partitions: List[PartitionLocation]
    use_external_partitions: bool = False
    recursive_scan_required: bool = False
    capability_group: str = "basic_layouts"
    crosses_buckets: bool = False
    crosses_regions: bool = False
    shares_partitions: bool = False
    base_name: str = field(default="")

    def __post_init__(self):
        """Derive base_name from name if not provided."""
        if not self.base_name:
            # Strip common suffixes for cleaner table names
            self.base_name = self.name.replace("_table", "").replace("_partition_table", "")

    def get_table_name(self, access_suffix: str) -> str:
        """Generate table name for a specific access method.

        Args:
            access_suffix: Access method suffix (e.g., "_glue_hive", "_uc_delta")

        Returns:
            Table name like "standard_glue_hive"
        """
        return f"{self.base_name}{access_suffix}"

    def supports_access_method(self, config: "AccessMethodConfig") -> tuple[bool, str]:
        """Check if this scenario can be tested with an access method.

        Args:
            config: AccessMethodConfig to check compatibility with

        Returns:
            Tuple of (is_supported, reason_if_not)
        """
        # Import here to avoid circular dependency
        from .access_methods import can_test_scenario_with_method

        return can_test_scenario_with_method(
            scenario_has_external_partitions=self.use_external_partitions,
            scenario_crosses_buckets=self.crosses_buckets,
            access_config=config,
        )

    def get_s3_path_for_method(self, bucket: str, s3_path_prefix: str) -> str:
        """Generate S3 path for this scenario under a specific access method.

        Args:
            bucket: S3 bucket name
            s3_path_prefix: Access method's S3 path prefix (e.g., "glue_hive")

        Returns:
            S3 path like "s3://bucket/tables/glue_hive/standard/"
        """
        return f"s3://{bucket}/tables/{s3_path_prefix}/{self.base_name}/"


# Basic Layouts - Scenario 1: Partitions under table root
PARTITIONS_UNDER_TABLE_ROOT = HiveTableScenario(
    name="standard_table",
    description="Files in single parent folder with standard Hive partitioning",
    table_location=f"s3://{BUCKET_EAST_1A}/tables/standard/",
    partitions=[
        PartitionLocation("region", "us-east", f"s3://{BUCKET_EAST_1A}/tables/standard/region=us-east/"),
        PartitionLocation("region", "us-west", f"s3://{BUCKET_EAST_1A}/tables/standard/region=us-west/"),
    ],
    use_external_partitions=False,
    recursive_scan_required=False,
    capability_group="basic_layouts",
    crosses_buckets=False,
    crosses_regions=False,
    shares_partitions=False,
)

# Basic Layouts - Scenario 2: Partitions in nested subdirectories
PARTITIONS_IN_NESTED_SUBDIRECTORIES = HiveTableScenario(
    name="recursive_table",
    description="Non-partition subfolders requiring recursive file discovery",
    table_location=f"s3://{BUCKET_EAST_1A}/tables/recursive/",
    partitions=[],  # Not using Hive partitions, just nested directories
    use_external_partitions=False,
    recursive_scan_required=True,
    capability_group="basic_layouts",
    crosses_buckets=False,
    crosses_regions=False,
    shares_partitions=False,
)

# External Partitions - Scenario 3: Partitions outside table root
PARTITIONS_OUTSIDE_TABLE_ROOT = HiveTableScenario(
    name="scattered_table",
    description="Partitions scattered across different locations in same bucket",
    table_location=f"s3://{BUCKET_EAST_1A}/tables/scattered/",
    partitions=[
        PartitionLocation("region", "us-east", f"s3://{BUCKET_EAST_1A}/tables/scattered/region=us-east/"),
        PartitionLocation("region", "us-west", f"s3://{BUCKET_EAST_1A}/data/region=us-west/"),  # Different path
    ],
    use_external_partitions=True,
    recursive_scan_required=False,
    capability_group="external_partitions",
    crosses_buckets=False,
    crosses_regions=False,
    shares_partitions=False,
)

# Multi-Location - Scenario 4: Partitions across S3 buckets
PARTITIONS_ACROSS_S3_BUCKETS = HiveTableScenario(
    name="cross_bucket_table",
    description="Partitions spanning multiple S3 buckets in us-east-1",
    table_location=f"s3://{BUCKET_EAST_1A}/tables/cross-bucket/",
    partitions=[
        PartitionLocation("region", "us-east", f"s3://{BUCKET_EAST_1A}/tables/cross-bucket/region=us-east/"),
        PartitionLocation("region", "us-west", f"s3://{BUCKET_EAST_1B}/tables/cross-bucket/region=us-west/"),
    ],
    use_external_partitions=True,
    recursive_scan_required=False,
    capability_group="multi_location",
    crosses_buckets=True,  # KEY: spans multiple S3 buckets
    crosses_regions=False,
    shares_partitions=False,
)

# Multi-Location - Scenario 5: Partitions across AWS regions
PARTITIONS_ACROSS_AWS_REGIONS = HiveTableScenario(
    name="cross_region_table",
    description="Partitions spanning us-east-1 and us-west-2 regions",
    table_location=f"s3://{BUCKET_EAST_1A}/tables/cross-region/",
    partitions=[
        PartitionLocation("region", "us-east", f"s3://{BUCKET_EAST_1A}/tables/cross-region/region=us-east/"),
        PartitionLocation("region", "us-west", f"s3://{BUCKET_WEST_2}/tables/cross-region/region=us-west/"),
    ],
    use_external_partitions=True,
    recursive_scan_required=False,
    capability_group="multi_location",
    crosses_buckets=True,  # Also crosses buckets (different bucket in different region)
    crosses_regions=True,  # KEY: spans AWS regions
    shares_partitions=False,
)

# Multi-Location - Scenario 6: Partitions shared between tables
PARTITIONS_SHARED_BETWEEN_TABLES_A = HiveTableScenario(
    name="shared_partition_table_a",
    description="First table using shared partition",
    table_location=f"s3://{BUCKET_EAST_1A}/tables/shared-a/",
    partitions=[
        PartitionLocation("region", "us-east", f"s3://{BUCKET_EAST_1A}/tables/shared-a/region=us-east/"),
        PartitionLocation("region", "shared", f"s3://{BUCKET_EAST_1A}/shared-data/region=shared/"),
    ],
    use_external_partitions=True,
    recursive_scan_required=False,
    capability_group="multi_location",
    crosses_buckets=False,
    crosses_regions=False,
    shares_partitions=True,  # KEY: shares partition with table B
    base_name="shared_a",  # Custom base name for cleaner table names
)

PARTITIONS_SHARED_BETWEEN_TABLES_B = HiveTableScenario(
    name="shared_partition_table_b",
    description="Second table using shared partition",
    table_location=f"s3://{BUCKET_EAST_1A}/tables/shared-b/",
    partitions=[
        PartitionLocation("region", "us-west", f"s3://{BUCKET_EAST_1A}/tables/shared-b/region=us-west/"),
        PartitionLocation("region", "shared", f"s3://{BUCKET_EAST_1A}/shared-data/region=shared/"),
    ],
    use_external_partitions=True,
    recursive_scan_required=False,
    capability_group="multi_location",
    crosses_buckets=False,
    crosses_regions=False,
    shares_partitions=True,  # KEY: shares partition with table A
    base_name="shared_b",  # Custom base name for cleaner table names
)

# Capability group collections
BASIC_LAYOUTS = [
    PARTITIONS_UNDER_TABLE_ROOT,
    PARTITIONS_IN_NESTED_SUBDIRECTORIES,
]

EXTERNAL_PARTITIONS = [
    PARTITIONS_OUTSIDE_TABLE_ROOT,
]

MULTI_LOCATION = [
    PARTITIONS_ACROSS_S3_BUCKETS,
    PARTITIONS_ACROSS_AWS_REGIONS,
    PARTITIONS_SHARED_BETWEEN_TABLES_A,
    PARTITIONS_SHARED_BETWEEN_TABLES_B,
]

# All scenarios for iteration
ALL_SCENARIOS = [
    PARTITIONS_UNDER_TABLE_ROOT,
    PARTITIONS_IN_NESTED_SUBDIRECTORIES,
    PARTITIONS_OUTSIDE_TABLE_ROOT,
    PARTITIONS_ACROSS_S3_BUCKETS,
    PARTITIONS_ACROSS_AWS_REGIONS,
    PARTITIONS_SHARED_BETWEEN_TABLES_A,
    PARTITIONS_SHARED_BETWEEN_TABLES_B,
]

# Backward-compatible aliases (deprecated - use descriptive names instead)
STANDARD_TABLE = PARTITIONS_UNDER_TABLE_ROOT
RECURSIVE_TABLE = PARTITIONS_IN_NESTED_SUBDIRECTORIES
SCATTERED_TABLE = PARTITIONS_OUTSIDE_TABLE_ROOT
CROSS_BUCKET_TABLE = PARTITIONS_ACROSS_S3_BUCKETS
CROSS_REGION_TABLE = PARTITIONS_ACROSS_AWS_REGIONS
SHARED_PARTITION_TABLE_A = PARTITIONS_SHARED_BETWEEN_TABLES_A
SHARED_PARTITION_TABLE_B = PARTITIONS_SHARED_BETWEEN_TABLES_B

# Backward-compatible phase groupings (deprecated - use capability groups instead)
PHASE_1_SCENARIOS = BASIC_LAYOUTS
PHASE_2_SCENARIOS = EXTERNAL_PARTITIONS + MULTI_LOCATION

# Mapping from base_name to scenario for lookup
SCENARIO_BY_BASE_NAME = {s.base_name: s for s in ALL_SCENARIOS}


def get_scenario_by_base_name(base_name: str) -> Optional[HiveTableScenario]:
    """Get scenario by its base name.

    Args:
        base_name: Short name like "standard", "scattered", "shared_a"

    Returns:
        HiveTableScenario or None if not found
    """
    return SCENARIO_BY_BASE_NAME.get(base_name)


def get_scenarios_for_access_method(config: "AccessMethodConfig") -> list[HiveTableScenario]:
    """Get all scenarios compatible with an access method.

    Args:
        config: AccessMethodConfig to check compatibility

    Returns:
        List of compatible scenarios
    """
    compatible = []
    for scenario in ALL_SCENARIOS:
        supported, _ = scenario.supports_access_method(config)
        if supported:
            compatible.append(scenario)
    return compatible


def get_incompatible_scenarios(config: "AccessMethodConfig") -> list[tuple[HiveTableScenario, str]]:
    """Get scenarios that are NOT compatible with an access method.

    Args:
        config: AccessMethodConfig to check

    Returns:
        List of (scenario, reason) tuples for incompatible scenarios
    """
    incompatible = []
    for scenario in ALL_SCENARIOS:
        supported, reason = scenario.supports_access_method(config)
        if not supported:
            incompatible.append((scenario, reason))
    return incompatible
