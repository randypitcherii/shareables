"""Discovery protocols and implementations for finding Hive tables to convert."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Protocol, runtime_checkable

from hive_to_delta.glue import get_glue_table_metadata, list_glue_tables
from hive_to_delta.models import TableInfo


@runtime_checkable
class Discovery(Protocol):
    """Protocol for table discovery strategies."""

    def discover(self, spark: Any) -> list[TableInfo]:
        """Discover tables to be converted.

        Args:
            spark: SparkSession (unused by some implementations, but part
                   of the protocol for strategies that need it).

        Returns:
            List of TableInfo objects describing discovered tables.
        """
        ...


@dataclass
class GlueDiscovery:
    """Discover tables by querying the AWS Glue Data Catalog.

    Args:
        database: Glue database name to search.
        pattern: Optional glob pattern to filter table names.
        region: AWS region for the Glue client.
    """

    database: str
    pattern: Optional[str] = None
    region: str = "us-east-1"

    def discover(self, spark: Any) -> list[TableInfo]:
        """Discover tables from Glue and return their metadata as TableInfo.

        Args:
            spark: SparkSession (not used, but satisfies the Discovery protocol).

        Returns:
            List of TableInfo objects with columns and partition keys from Glue.
        """
        table_names = list_glue_tables(
            self.database, pattern=self.pattern, region=self.region
        )

        results: list[TableInfo] = []
        for name in table_names:
            meta = get_glue_table_metadata(self.database, name, self.region)

            location = meta["StorageDescriptor"]["Location"].rstrip("/")
            data_columns: list[dict[str, str]] = meta["StorageDescriptor"]["Columns"]
            partition_keys_raw: list[dict[str, str]] = meta.get("PartitionKeys", [])

            partition_key_names = [pk["Name"] for pk in partition_keys_raw]
            all_columns = data_columns + partition_keys_raw

            results.append(
                TableInfo(
                    name=name,
                    location=location,
                    columns=all_columns,
                    partition_keys=partition_key_names,
                )
            )

        return results
