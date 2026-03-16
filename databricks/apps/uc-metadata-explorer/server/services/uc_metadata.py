from __future__ import annotations

import os
from urllib.parse import urlparse

from databricks import sql

from ..config import get_workspace_client, get_workspace_host, settings
from ..models import (
    ColumnMetadata,
    MetadataObjectDetail,
    MetadataObjectSummary,
    TagKV,
)


class MetadataPrimaryUnavailableError(RuntimeError):
    def __init__(self, *, message: str, reason_code: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def _server_hostname() -> str:
    host = get_workspace_host()
    parsed = urlparse(host)
    return parsed.hostname or host.replace("https://", "").replace("http://", "")


def _http_path() -> str:
    from_env = os.getenv("DATABRICKS_SQL_HTTP_PATH")
    if from_env:
        return from_env
    if not settings.sql_warehouse_id:
        raise ValueError("Missing DATABRICKS_SQL_WAREHOUSE_ID (or WAREHOUSE_ID) for SQL access.")
    return f"/sql/1.0/warehouses/{settings.sql_warehouse_id}"


def _run_query(access_token: str, statement: str) -> list[dict]:
    if not access_token:
        raise ValueError("Missing access token for Databricks SQL query.")
    with sql.connect(
        server_hostname=_server_hostname(),
        http_path=_http_path(),
        access_token=access_token,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(statement)
            rows = cursor.fetchall()
            columns = [item[0] for item in (cursor.description or [])]
    return [dict(zip(columns, row)) for row in rows]


def _status_text(value: object) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").upper()


def _warehouse_is_queryable() -> bool:
    """Fast preflight check to avoid connector hangs on broken warehouses."""

    if not settings.sql_warehouse_id:
        return False
    try:
        warehouse = get_workspace_client().warehouses.get(settings.sql_warehouse_id)
    except Exception:
        return False

    # Only RUNNING is considered safe for connector queries.
    return _status_text(getattr(warehouse, "state", "")) == "RUNNING"


def _fallback_list_objects_from_uc_api(*, search: str, gap_only: bool, limit: int) -> list[MetadataObjectSummary]:
    client = get_workspace_client()
    search_lc = search.lower().strip()
    results: list[MetadataObjectSummary] = []

    catalog_names = [c.name for c in client.catalogs.list() if c.name and c.name != "system"]
    if search_lc:
        matching_catalogs = [c for c in catalog_names if search_lc in c.lower()]
        if matching_catalogs:
            catalog_names = matching_catalogs

    for catalog_name in catalog_names:
        for schema in client.schemas.list(catalog_name=catalog_name):
            schema_name = schema.name or ""
            if not schema_name or schema_name == "information_schema":
                continue
            for table in client.tables.list_summaries(
                catalog_name=catalog_name,
                schema_name_pattern=schema_name,
                table_name_pattern=f"*{search_lc}*" if search_lc and "." not in search_lc else None,
                max_results=max(limit * 3, 100),
            ):
                full_name = table.full_name or ""
                object_name = full_name.split(".")[-1] if full_name else (table.name or "")
                owner = getattr(table, "owner", None)
                comment = getattr(table, "comment", None)
                table_type = getattr(table, "table_type", None)
                object_type = getattr(table_type, "value", str(table_type or "TABLE"))

                haystack = f"{catalog_name}.{schema_name}.{object_name}".lower()
                if search_lc and search_lc not in haystack:
                    continue

                table_description_missing = not bool((comment or "").strip())
                table_owner_missing = not bool((owner or "").strip())
                total_gap_score = int(table_description_missing) + int(table_owner_missing)

                if gap_only and total_gap_score == 0:
                    continue

                results.append(
                    MetadataObjectSummary(
                        catalog_name=catalog_name,
                        schema_name=schema_name,
                        object_name=object_name,
                        object_type=object_type,
                        owner=owner,
                        comment=comment,
                        table_tag_count=0,
                        column_count=0,
                        columns_missing_description=0,
                        columns_missing_tags=0,
                        table_description_missing=table_description_missing,
                        table_owner_missing=table_owner_missing,
                        table_tags_missing=False,
                        total_gap_score=total_gap_score,
                    )
                )

                if len(results) >= limit:
                    return results

    return results


def _list_objects_from_fallback(*, search: str, gap_only: bool, limit: int) -> list[MetadataObjectSummary]:
    return _fallback_list_objects_from_uc_api(search=search, gap_only=gap_only, limit=limit)


def list_objects_fallback(*, search: str, gap_only: bool, limit: int) -> list[MetadataObjectSummary]:
    return _list_objects_from_fallback(search=search, gap_only=gap_only, limit=limit)


def list_objects(
    *,
    access_token: str,
    search: str,
    gap_only: bool,
    limit: int,
) -> list[MetadataObjectSummary]:
    if not settings.sql_warehouse_id or not _warehouse_is_queryable():
        return _list_objects_from_fallback(search=search, gap_only=gap_only, limit=limit)

    try:
        return _list_objects_from_sql(
            access_token=access_token,
            search=search,
            gap_only=gap_only,
            limit=limit,
        )
    except Exception:
        return _list_objects_from_fallback(search=search, gap_only=gap_only, limit=limit)


def list_objects_primary(
    *,
    access_token: str,
    search: str,
    gap_only: bool,
    limit: int,
) -> list[MetadataObjectSummary]:
    if not settings.sql_warehouse_id:
        raise MetadataPrimaryUnavailableError(
            message=(
                "Primary metadata loading is unavailable because no SQL warehouse is configured. "
                "Set DATABRICKS_SQL_WAREHOUSE_ID or WAREHOUSE_ID."
            ),
            reason_code="warehouse_not_configured",
        )
    if not _warehouse_is_queryable():
        raise MetadataPrimaryUnavailableError(
            message=(
                "Primary metadata loading failed because the SQL warehouse is unavailable or not running."
            ),
            reason_code="warehouse_unavailable",
        )

    return _list_objects_from_sql(
        access_token=access_token,
        search=search,
        gap_only=gap_only,
        limit=limit,
    )


def _list_objects_from_sql(
    *,
    access_token: str,
    search: str,
    gap_only: bool,
    limit: int,
) -> list[MetadataObjectSummary]:

    search_filter = ""
    if search:
        escaped = _sql_string(search.lower())
        search_filter = f"""
          AND (
            LOWER(t.table_catalog) LIKE '%{escaped}%'
            OR LOWER(t.table_schema) LIKE '%{escaped}%'
            OR LOWER(t.table_name) LIKE '%{escaped}%'
          )
        """

    gap_filter = ""
    if gap_only:
        gap_filter = """
          AND (
            COALESCE(NULLIF(TRIM(t.comment), ''), '') = ''
            OR COALESCE(NULLIF(TRIM(t.table_owner), ''), '') = ''
            OR COALESCE(tt.table_tag_count, 0) = 0
            OR COALESCE(cs.columns_missing_description, 0) > 0
            OR COALESCE(cs.columns_missing_tags, 0) > 0
          )
        """

    statement = f"""
    WITH table_tags AS (
      SELECT
        catalog_name,
        schema_name,
        table_name,
        COUNT(*) AS table_tag_count
      FROM system.information_schema.table_tags
      GROUP BY catalog_name, schema_name, table_name
    ),
    column_tags AS (
      SELECT
        catalog_name,
        schema_name,
        table_name,
        column_name,
        COUNT(*) AS column_tag_count
      FROM system.information_schema.column_tags
      GROUP BY catalog_name, schema_name, table_name, column_name
    ),
    column_stats AS (
      SELECT
        c.table_catalog,
        c.table_schema,
        c.table_name,
        COUNT(*) AS column_count,
        SUM(CASE WHEN COALESCE(NULLIF(TRIM(c.comment), ''), '') = '' THEN 1 ELSE 0 END) AS columns_missing_description,
        SUM(CASE WHEN COALESCE(ct.column_tag_count, 0) = 0 THEN 1 ELSE 0 END) AS columns_missing_tags
      FROM system.information_schema.columns c
      LEFT JOIN column_tags ct
        ON ct.catalog_name = c.table_catalog
        AND ct.schema_name = c.table_schema
        AND ct.table_name = c.table_name
        AND ct.column_name = c.column_name
      GROUP BY c.table_catalog, c.table_schema, c.table_name
    )
    SELECT
      t.table_catalog AS catalog_name,
      t.table_schema AS schema_name,
      t.table_name AS object_name,
      t.table_type AS object_type,
      t.table_owner AS owner,
      t.comment AS comment,
      COALESCE(tt.table_tag_count, 0) AS table_tag_count,
      COALESCE(cs.column_count, 0) AS column_count,
      COALESCE(cs.columns_missing_description, 0) AS columns_missing_description,
      COALESCE(cs.columns_missing_tags, 0) AS columns_missing_tags,
      CASE WHEN COALESCE(NULLIF(TRIM(t.comment), ''), '') = '' THEN TRUE ELSE FALSE END AS table_description_missing,
      CASE WHEN COALESCE(NULLIF(TRIM(t.table_owner), ''), '') = '' THEN TRUE ELSE FALSE END AS table_owner_missing,
      CASE WHEN COALESCE(tt.table_tag_count, 0) = 0 THEN TRUE ELSE FALSE END AS table_tags_missing,
      (
        CASE WHEN COALESCE(NULLIF(TRIM(t.comment), ''), '') = '' THEN 1 ELSE 0 END
        + CASE WHEN COALESCE(NULLIF(TRIM(t.table_owner), ''), '') = '' THEN 1 ELSE 0 END
        + CASE WHEN COALESCE(tt.table_tag_count, 0) = 0 THEN 1 ELSE 0 END
        + COALESCE(cs.columns_missing_description, 0)
        + COALESCE(cs.columns_missing_tags, 0)
      ) AS total_gap_score
    FROM system.information_schema.tables t
    LEFT JOIN table_tags tt
      ON tt.catalog_name = t.table_catalog
      AND tt.schema_name = t.table_schema
      AND tt.table_name = t.table_name
    LEFT JOIN column_stats cs
      ON cs.table_catalog = t.table_catalog
      AND cs.table_schema = t.table_schema
      AND cs.table_name = t.table_name
    WHERE t.table_schema <> 'information_schema'
      {search_filter}
      {gap_filter}
    ORDER BY total_gap_score DESC, catalog_name, schema_name, object_name
    LIMIT {limit}
    """

    rows = _run_query(access_token, statement)
    return [MetadataObjectSummary(**row) for row in rows]


def get_object_detail(
    *,
    access_token: str,
    catalog_name: str,
    schema_name: str,
    object_name: str,
) -> MetadataObjectDetail:
    escaped_catalog = _sql_string(catalog_name)
    escaped_schema = _sql_string(schema_name)
    escaped_object = _sql_string(object_name)

    summary_rows = list_objects(
        access_token=access_token,
        search=f"{catalog_name}.{schema_name}.{object_name}",
        gap_only=False,
        limit=1000,
    )
    summary = next(
        (
            row
            for row in summary_rows
            if row.catalog_name == catalog_name
            and row.schema_name == schema_name
            and row.object_name == object_name
        ),
        None,
    )
    if summary is None:
        raise ValueError(f"Object not found: {catalog_name}.{schema_name}.{object_name}")

    table_tags_rows = _run_query(
        access_token,
        f"""
        SELECT tag_name AS name, tag_value AS value
        FROM system.information_schema.table_tags
        WHERE catalog_name = '{escaped_catalog}'
          AND schema_name = '{escaped_schema}'
          AND table_name = '{escaped_object}'
        ORDER BY tag_name
        """,
    )
    table_tags = [TagKV(**row) for row in table_tags_rows]

    columns_rows = _run_query(
        access_token,
        f"""
        SELECT
          c.column_name,
          c.full_data_type AS data_type,
          c.comment
        FROM system.information_schema.columns c
        WHERE c.table_catalog = '{escaped_catalog}'
          AND c.table_schema = '{escaped_schema}'
          AND c.table_name = '{escaped_object}'
        ORDER BY c.ordinal_position
        """,
    )
    column_tag_rows = _run_query(
        access_token,
        f"""
        SELECT column_name, tag_name AS name, tag_value AS value
        FROM system.information_schema.column_tags
        WHERE catalog_name = '{escaped_catalog}'
          AND schema_name = '{escaped_schema}'
          AND table_name = '{escaped_object}'
        ORDER BY column_name, tag_name
        """,
    )
    tags_by_column: dict[str, list[TagKV]] = {}
    for row in column_tag_rows:
        tags_by_column.setdefault(row["column_name"], []).append(TagKV(name=row["name"], value=row["value"]))

    columns: list[ColumnMetadata] = []
    for row in columns_rows:
        tags = tags_by_column.get(row["column_name"], [])
        comment = row.get("comment")
        columns.append(
            ColumnMetadata(
                column_name=row["column_name"],
                data_type=row["data_type"],
                comment=comment,
                tags=tags,
                description_missing=not bool((comment or "").strip()),
                tags_missing=len(tags) == 0,
            )
        )

    return MetadataObjectDetail(summary=summary, table_tags=table_tags, columns=columns)
