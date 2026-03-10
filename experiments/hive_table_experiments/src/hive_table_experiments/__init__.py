"""Hive Table Experiments: Databricks Glue Catalog Compatibility Validation

Provides structured testing of three Glue table access methods:
1. glue_as_hive_metastore - Spark's built-in Glue integration
2. uc_glue_federation - Unity Catalog foreign catalog pointing to Glue
3. uc_external_tables - Manual UC external tables (Parquet or Delta)
"""

__version__ = "0.2.0"

from .access_methods import (
    AccessMethod,
    AccessMethodConfig,
    TableFormat,
    GLUE_AS_HIVE,
    UC_GLUE_FEDERATION,
    UC_EXTERNAL_HIVE,
    UC_EXTERNAL_DELTA,
    ALL_ACCESS_METHODS,
    ACCESS_METHOD_BY_SUFFIX,
    get_access_method_config,
    can_test_scenario_with_method,
)

from .results import (
    Operation,
    OperationResult,
    ScenarioResult,
    MigrationMetrics,
    ResultsMatrix,
    get_results_matrix,
    reset_results_matrix,
)

from .scenarios import (
    ALL_SCENARIOS,
    BASIC_LAYOUTS,
    EXTERNAL_PARTITIONS,
    MULTI_LOCATION,
    HiveTableScenario,
    PartitionLocation,
    SCENARIO_BY_BASE_NAME,
    get_scenario_by_base_name,
    get_scenarios_for_access_method,
    get_incompatible_scenarios,
    # Bucket constants for configuration
    BUCKET_EAST_1A,
    BUCKET_EAST_1B,
    BUCKET_WEST_2,
)

from .validation import (
    get_glue_client,
    get_s3_client,
    get_partition_location,
    get_table_location,
    get_glue_partitions,
    list_s3_files,
    count_s3_parquet_files,
    verify_partition_exists_in_glue,
    verify_s3_files_exist,
    WriteValidationResult,
)

from .fixtures import (
    setup_fixtures_for_access_method,
    setup_all_access_methods,
    get_table_info_for_scenario,
    # Legacy functions
    setup_basic_layout_fixtures,
    setup_external_and_multi_location_fixtures,
)

from .conversion_fixtures import (
    CONVERSION_METHODS,
    ConversionSourceTable,
    setup_conversion_source_tables,
    get_conversion_source_info,
    get_conversion_table_name,
    get_conversion_s3_path,
    list_all_conversion_sources,
)

from .conversion_matrix import (
    ConversionResult,
    PostConversionResult,
    ConversionMatrix,
    get_conversion_matrix,
    reset_conversion_matrix,
    record_conversion_result,
    record_post_conversion_result,
)

from .delta_converter import (
    convert_hive_to_delta_uc,
    validate_delta_table,
    ConversionResult as DeltaConversionResult,
    ValidationResult as DeltaValidationResult,
    get_glue_table_metadata,
    GlueTableMetadata,
    cleanup_delta_log,
)

from .external_vacuum import (
    vacuum_external_files,
    vacuum_external_files_pure_python,
    get_orphaned_external_files_spark,
    get_delta_log_from_s3,
    find_orphaned_external_paths,
    list_external_remove_actions,
    check_external_files_exist,
    generate_orphaned_files_query,
    RemoveAction,
    AddAction,
    ExternalVacuumResult,
)

__all__ = [
    # Access Methods
    "AccessMethod",
    "AccessMethodConfig",
    "TableFormat",
    "GLUE_AS_HIVE",
    "UC_GLUE_FEDERATION",
    "UC_EXTERNAL_HIVE",
    "UC_EXTERNAL_DELTA",
    "ALL_ACCESS_METHODS",
    "ACCESS_METHOD_BY_SUFFIX",
    "get_access_method_config",
    "can_test_scenario_with_method",
    # Results
    "Operation",
    "OperationResult",
    "ScenarioResult",
    "MigrationMetrics",
    "ResultsMatrix",
    "get_results_matrix",
    "reset_results_matrix",
    # Scenarios
    "ALL_SCENARIOS",
    "BASIC_LAYOUTS",
    "EXTERNAL_PARTITIONS",
    "MULTI_LOCATION",
    "HiveTableScenario",
    "PartitionLocation",
    "SCENARIO_BY_BASE_NAME",
    "get_scenario_by_base_name",
    "get_scenarios_for_access_method",
    "get_incompatible_scenarios",
    # Bucket Constants
    "BUCKET_EAST_1A",
    "BUCKET_EAST_1B",
    "BUCKET_WEST_2",
    # Validation
    "get_glue_client",
    "get_s3_client",
    "get_partition_location",
    "get_table_location",
    "get_glue_partitions",
    "list_s3_files",
    "count_s3_parquet_files",
    "verify_partition_exists_in_glue",
    "verify_s3_files_exist",
    "WriteValidationResult",
    # Fixtures
    "setup_fixtures_for_access_method",
    "setup_all_access_methods",
    "get_table_info_for_scenario",
    "setup_basic_layout_fixtures",
    "setup_external_and_multi_location_fixtures",
    # Conversion Fixtures
    "CONVERSION_METHODS",
    "ConversionSourceTable",
    "setup_conversion_source_tables",
    "get_conversion_source_info",
    "get_conversion_table_name",
    "get_conversion_s3_path",
    "list_all_conversion_sources",
    # Conversion Matrix
    "ConversionResult",
    "PostConversionResult",
    "ConversionMatrix",
    "get_conversion_matrix",
    "reset_conversion_matrix",
    "record_conversion_result",
    "record_post_conversion_result",
    # Delta Converter (Hive to Delta UC)
    "convert_hive_to_delta_uc",
    "validate_delta_table",
    "DeltaConversionResult",
    "DeltaValidationResult",
    "get_glue_table_metadata",
    "GlueTableMetadata",
    "cleanup_delta_log",
    # External Vacuum (cleanup orphaned files)
    "vacuum_external_files",
    "vacuum_external_files_pure_python",
    "get_orphaned_external_files_spark",
    "get_delta_log_from_s3",
    "find_orphaned_external_paths",
    "list_external_remove_actions",
    "check_external_files_exist",
    "generate_orphaned_files_query",
    "RemoveAction",
    "AddAction",
    "ExternalVacuumResult",
]
