"""Discovery protocols and implementations for finding Hive tables to convert."""

from __future__ import annotations

import fnmatch
import logging
from dataclasses import dataclass, field
from typing import Any, Optional, Protocol, runtime_checkable

from hive_to_delta.glue import get_glue_table_metadata, list_glue_tables
from hive_to_delta.models import TableInfo

logger = logging.getLogger(__name__)


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


@dataclass
class UCDiscovery:
    """Discover tables from Unity Catalog / hive_metastore.

    Uses Spark catalog APIs to enumerate tables and extract metadata.
    Works with any catalog accessible from the SparkSession, including
    hive_metastore for legacy Hive tables.

    Args:
        allow: Required list of FQN patterns (catalog.schema.table).
            Supports glob patterns: "hive_metastore.my_db.*", "catalog.*.dim_*"
        deny: Optional list of FQN patterns to exclude.
            Defaults to ["*.information_schema.*"].
    """

    allow: list[str]
    deny: list[str] = field(default_factory=lambda: ["*.information_schema.*"])

    @staticmethod
    def _matches_fqn(fqn: str, patterns: list[str]) -> bool:
        """Return True if fqn matches any pattern using fnmatch."""
        return any(fnmatch.fnmatch(fqn, p) for p in patterns)

    def _should_include(self, fqn: str) -> bool:
        """Return True if fqn matches allow AND does NOT match deny."""
        return self._matches_fqn(fqn, self.allow) and not self._matches_fqn(
            fqn, self.deny
        )

    def _extract_catalog_schema_pairs(self, spark: Any) -> list[tuple[str, str]]:
        """Parse allow patterns to find catalog.schema combinations to query.

        Expands wildcard catalogs via SHOW CATALOGS and wildcard schemas
        via SHOW SCHEMAS IN {catalog}.
        """
        pairs: list[tuple[str, str]] = []
        seen_catalogs: dict[str, list[str]] = {}

        for pattern in self.allow:
            parts = pattern.split(".")
            if len(parts) != 3:
                continue
            cat_pat, schema_pat, _table_pat = parts

            if cat_pat not in seen_catalogs:
                if "*" in cat_pat or "?" in cat_pat or "[" in cat_pat:
                    rows = spark.sql("SHOW CATALOGS").collect()
                    seen_catalogs[cat_pat] = [
                        r[0]
                        for r in rows
                        if fnmatch.fnmatch(r[0], cat_pat)
                    ]
                else:
                    seen_catalogs[cat_pat] = [cat_pat]

            for catalog in seen_catalogs[cat_pat]:
                if "*" in schema_pat or "?" in schema_pat or "[" in schema_pat:
                    try:
                        schema_rows = spark.sql(
                            f"SHOW SCHEMAS IN `{catalog}`"
                        ).collect()
                        for row in schema_rows:
                            schema_name = row[0]
                            if fnmatch.fnmatch(schema_name, schema_pat):
                                if (catalog, schema_name) not in pairs:
                                    pairs.append((catalog, schema_name))
                    except Exception:
                        logger.warning(
                            "Failed to list schemas in catalog %s", catalog
                        )
                else:
                    if (catalog, schema_pat) not in pairs:
                        pairs.append((catalog, schema_pat))

        return pairs

    def discover(self, spark: Any) -> list[TableInfo]:
        """Discover tables from Unity Catalog and return their metadata.

        Args:
            spark: SparkSession with access to the target catalogs.

        Returns:
            List of TableInfo objects with location and partition keys.
        """
        results: list[TableInfo] = []
        pairs = self._extract_catalog_schema_pairs(spark)

        for catalog, schema in pairs:
            try:
                table_rows = spark.sql(
                    f"SHOW TABLES IN `{catalog}`.`{schema}`"
                ).collect()
            except Exception:
                logger.warning(
                    "Failed to list tables in %s.%s", catalog, schema
                )
                continue

            for row in table_rows:
                table_name = row["tableName"]
                fqn = f"{catalog}.{schema}.{table_name}"

                if not self._should_include(fqn):
                    continue

                try:
                    desc_rows = spark.sql(
                        f"DESCRIBE TABLE EXTENDED `{catalog}`.`{schema}`.`{table_name}`"
                    ).collect()

                    location = ""
                    for desc_row in desc_rows:
                        if desc_row["col_name"] == "Location":
                            location = desc_row["data_type"]
                            break

                    col_list = spark.catalog.listColumns(
                        f"{catalog}.{schema}.{table_name}"
                    )
                    partition_key_names = [
                        c.name for c in col_list if c.isPartition
                    ]

                    results.append(
                        TableInfo(
                            name=table_name,
                            location=location.rstrip("/"),
                            columns=None,
                            partition_keys=partition_key_names,
                        )
                    )
                except Exception:
                    logger.warning("Failed to describe table %s, skipping", fqn)
                    continue

        return results
