from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from ..models import ColumnMetadata, MetadataObjectDetail, MetadataObjectSummary, TagKV


def _cache_path() -> Path:
    configured = os.getenv("UC_METADATA_SQLITE_PATH", ".cache/uc_metadata_cache.db")
    path = Path(configured)
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def _connect() -> sqlite3.Connection:
    path = _cache_path()
    if not path.exists():
        raise FileNotFoundError(f"SQLite cache not found at {path}. Build it with scripts/build-sqlite-cache.py.")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _to_summary(row: sqlite3.Row) -> MetadataObjectSummary:
    data = dict(row)
    for key in (
        "table_description_missing",
        "table_owner_missing",
        "table_tags_missing",
    ):
        data[key] = bool(data[key])
    return MetadataObjectSummary(**data)


def list_objects_cached(*, search: str, gap_only: bool, limit: int) -> list[MetadataObjectSummary]:
    clauses = []
    params: list[object] = []
    if search.strip():
        pattern = f"%{search.strip().lower()}%"
        clauses.append(
            "(LOWER(catalog_name) LIKE ? OR LOWER(schema_name) LIKE ? OR LOWER(object_name) LIKE ?)"
        )
        params.extend([pattern, pattern, pattern])
    if gap_only:
        clauses.append(
            "(table_description_missing = 1 OR table_owner_missing = 1 OR table_tags_missing = 1 "
            "OR columns_missing_description > 0 OR columns_missing_tags > 0)"
        )

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)

    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
              catalog_name,
              schema_name,
              object_name,
              object_type,
              owner,
              comment,
              table_tag_count,
              column_count,
              columns_missing_description,
              columns_missing_tags,
              table_description_missing,
              table_owner_missing,
              table_tags_missing,
              total_gap_score
            FROM metadata_objects
            {where_clause}
            ORDER BY total_gap_score DESC, catalog_name, schema_name, object_name
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [_to_summary(row) for row in rows]


def get_object_detail_cached(*, catalog_name: str, schema_name: str, object_name: str) -> MetadataObjectDetail:
    with _connect() as conn:
        summary_row = conn.execute(
            """
            SELECT
              catalog_name,
              schema_name,
              object_name,
              object_type,
              owner,
              comment,
              table_tag_count,
              column_count,
              columns_missing_description,
              columns_missing_tags,
              table_description_missing,
              table_owner_missing,
              table_tags_missing,
              total_gap_score
            FROM metadata_objects
            WHERE catalog_name = ? AND schema_name = ? AND object_name = ?
            """,
            (catalog_name, schema_name, object_name),
        ).fetchone()
        if summary_row is None:
            raise ValueError(f"Object not found in cache: {catalog_name}.{schema_name}.{object_name}")
        summary = _to_summary(summary_row)

        table_tags_rows = conn.execute(
            """
            SELECT tag_name AS name, tag_value AS value
            FROM metadata_table_tags
            WHERE catalog_name = ? AND schema_name = ? AND object_name = ?
            ORDER BY tag_name
            """,
            (catalog_name, schema_name, object_name),
        ).fetchall()
        table_tags = [TagKV(name=row["name"], value=row["value"]) for row in table_tags_rows]

        columns_rows = conn.execute(
            """
            SELECT column_name, data_type, comment
            FROM metadata_columns
            WHERE catalog_name = ? AND schema_name = ? AND object_name = ?
            ORDER BY ordinal_position
            """,
            (catalog_name, schema_name, object_name),
        ).fetchall()
        column_tags_rows = conn.execute(
            """
            SELECT column_name, tag_name AS name, tag_value AS value
            FROM metadata_column_tags
            WHERE catalog_name = ? AND schema_name = ? AND object_name = ?
            ORDER BY column_name, tag_name
            """,
            (catalog_name, schema_name, object_name),
        ).fetchall()

    tags_by_column: dict[str, list[TagKV]] = {}
    for row in column_tags_rows:
        tags_by_column.setdefault(row["column_name"], []).append(TagKV(name=row["name"], value=row["value"]))

    columns: list[ColumnMetadata] = []
    for row in columns_rows:
        tags = tags_by_column.get(row["column_name"], [])
        comment = row["comment"]
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
