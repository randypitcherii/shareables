"""
hive_to_delta - Bulk registration of AWS Glue Hive tables as external Delta tables in Unity Catalog.

API Tiers:

    Simple API - one call to convert a single table:
        from hive_to_delta import convert_table

    Composable API - mix and match discovery/listing strategies:
        from hive_to_delta import convert, GlueDiscovery, S3Listing

    Legacy API - original interface (still supported):
        from hive_to_delta import convert_single_table, convert_tables
"""

from hive_to_delta.converter import convert_table, convert, convert_single_table, convert_tables
from hive_to_delta.discovery import Discovery, GlueDiscovery, UCDiscovery
from hive_to_delta.listing import Listing, S3Listing, InventoryListing, validate_files_df
from hive_to_delta.models import TableInfo, ParquetFileInfo, ConversionResult
from hive_to_delta.glue import list_glue_tables
from hive_to_delta.vacuum import vacuum_external_files
from hive_to_delta.parallel import ConversionSummary, create_summary

__all__ = [
    # Simple API
    "convert_table",
    # Composable API
    "convert",
    "Discovery",
    "GlueDiscovery",
    "UCDiscovery",
    "Listing",
    "S3Listing",
    "InventoryListing",
    "validate_files_df",
    "TableInfo",
    "ParquetFileInfo",
    "ConversionResult",
    # Legacy API
    "convert_single_table",
    "convert_tables",
    # Utilities
    "vacuum_external_files",
    "list_glue_tables",
    "ConversionSummary",
    "create_summary",
]
__version__ = "0.2.0"
