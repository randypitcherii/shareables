#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

from databricks import sql
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _load_env() -> None:
    app_env = os.getenv("APP_ENV", "dev").lower()
    env_file = PROJECT_ROOT / f"{app_env}.env"
    if env_file.exists():
        load_dotenv(env_file, override=False)


def _config():
    from server import config  # imported after env load

    return config


def _warehouse_http_path() -> str:
    cfg = _config()
    explicit = os.getenv("DATABRICKS_SQL_HTTP_PATH")
    if explicit:
        return explicit
    if not cfg.settings.sql_warehouse_id:
        raise RuntimeError("Missing DATABRICKS_SQL_WAREHOUSE_ID in environment.")
    return f"/sql/1.0/warehouses/{cfg.settings.sql_warehouse_id}"


def _workspace_hostname() -> str:
    cfg = _config()
    host = cfg.get_workspace_host()
    parsed = urlparse(host)
    if parsed.hostname:
        return parsed.hostname
    return host.replace("https://", "").replace("http://", "")


def _access_token() -> str:
    cfg = _config()
    client = cfg.get_workspace_client()
    auth = client.config.authenticate()
    header = auth.get("Authorization", "")
    if not header.startswith("Bearer "):
        raise RuntimeError("Unable to resolve Databricks access token from current profile.")
    return header.replace("Bearer ", "")


def _run_query(token: str, statement: str) -> list[dict]:
    with sql.connect(
        server_hostname=_workspace_hostname(),
        http_path=_warehouse_http_path(),
        access_token=token,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(statement)
            rows = cursor.fetchall()
            columns = [item[0] for item in (cursor.description or [])]
    return [dict(zip(columns, row)) for row in rows]


def _sql_list(values: Iterable[str]) -> str:
    escaped = ["'" + v.replace("'", "''") + "'" for v in values if v]
    return ", ".join(escaped)


def _selected_tables_cte(limit: int, catalogs: list[str]) -> str:
    filters = [
        "t.table_schema <> 'information_schema'",
        "t.table_catalog <> 'system'",
        "t.table_catalog NOT LIKE '__databricks_internal%'",
    ]
    if catalogs:
        filters.append(f"t.table_catalog IN ({_sql_list(catalogs)})")
    return f"""
      selected_tables AS (
        SELECT
          t.table_catalog AS catalog_name,
          t.table_schema AS schema_name,
          t.table_name AS object_name,
          t.table_type AS object_type,
          t.table_owner AS owner,
          t.comment AS comment
        FROM system.information_schema.tables t
        WHERE {' AND '.join(filters)}
        ORDER BY t.table_catalog, t.table_schema, t.table_name
        LIMIT {limit}
      )
    """


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata_snapshot (
          snapshot_id INTEGER PRIMARY KEY,
          created_at_utc TEXT NOT NULL,
          source TEXT NOT NULL,
          warehouse_id TEXT NOT NULL,
          table_limit INTEGER NOT NULL,
          catalogs_filter TEXT
        );

        CREATE TABLE IF NOT EXISTS metadata_objects (
          catalog_name TEXT NOT NULL,
          schema_name TEXT NOT NULL,
          object_name TEXT NOT NULL,
          object_type TEXT NOT NULL,
          owner TEXT,
          comment TEXT,
          table_tag_count INTEGER NOT NULL,
          column_count INTEGER NOT NULL,
          columns_missing_description INTEGER NOT NULL,
          columns_missing_tags INTEGER NOT NULL,
          table_description_missing INTEGER NOT NULL,
          table_owner_missing INTEGER NOT NULL,
          table_tags_missing INTEGER NOT NULL,
          total_gap_score INTEGER NOT NULL,
          PRIMARY KEY (catalog_name, schema_name, object_name)
        );

        CREATE TABLE IF NOT EXISTS metadata_columns (
          catalog_name TEXT NOT NULL,
          schema_name TEXT NOT NULL,
          object_name TEXT NOT NULL,
          column_name TEXT NOT NULL,
          ordinal_position INTEGER NOT NULL,
          data_type TEXT NOT NULL,
          comment TEXT,
          PRIMARY KEY (catalog_name, schema_name, object_name, column_name)
        );

        CREATE TABLE IF NOT EXISTS metadata_table_tags (
          catalog_name TEXT NOT NULL,
          schema_name TEXT NOT NULL,
          object_name TEXT NOT NULL,
          tag_name TEXT NOT NULL,
          tag_value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS metadata_column_tags (
          catalog_name TEXT NOT NULL,
          schema_name TEXT NOT NULL,
          object_name TEXT NOT NULL,
          column_name TEXT NOT NULL,
          tag_name TEXT NOT NULL,
          tag_value TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_metadata_objects_gap ON metadata_objects(total_gap_score DESC);
        CREATE INDEX IF NOT EXISTS idx_metadata_columns_obj
          ON metadata_columns(catalog_name, schema_name, object_name);
        CREATE INDEX IF NOT EXISTS idx_metadata_table_tags_obj
          ON metadata_table_tags(catalog_name, schema_name, object_name);
        CREATE INDEX IF NOT EXISTS idx_metadata_column_tags_obj
          ON metadata_column_tags(catalog_name, schema_name, object_name, column_name);
        """
    )


def main() -> None:
    _load_env()

    parser = argparse.ArgumentParser(description="Build local SQLite cache from system.information_schema.")
    parser.add_argument("--out", default=".cache/uc_metadata_cache.db", help="SQLite output path.")
    parser.add_argument("--table-limit", type=int, default=5000, help="Max tables to snapshot.")
    parser.add_argument(
        "--catalogs",
        default="",
        help="Optional comma-separated catalog filter (e.g. main,prod).",
    )
    args = parser.parse_args()

    catalogs = [item.strip() for item in args.catalogs.split(",") if item.strip()]

    token = _access_token()
    cte = _selected_tables_cte(limit=args.table_limit, catalogs=catalogs)

    tables = _run_query(
        token,
        f"""
        WITH {cte}
        SELECT * FROM selected_tables
        """,
    )
    columns = _run_query(
        token,
        f"""
        WITH {cte}
        SELECT
          c.table_catalog AS catalog_name,
          c.table_schema AS schema_name,
          c.table_name AS object_name,
          c.column_name AS column_name,
          c.ordinal_position AS ordinal_position,
          c.full_data_type AS data_type,
          c.comment AS comment
        FROM system.information_schema.columns c
        INNER JOIN selected_tables st
          ON st.catalog_name = c.table_catalog
          AND st.schema_name = c.table_schema
          AND st.object_name = c.table_name
        ORDER BY c.table_catalog, c.table_schema, c.table_name, c.ordinal_position
        """,
    )
    table_tags = _run_query(
        token,
        f"""
        WITH {cte}
        SELECT
          tt.catalog_name,
          tt.schema_name,
          tt.table_name AS object_name,
          tt.tag_name,
          tt.tag_value
        FROM system.information_schema.table_tags tt
        INNER JOIN selected_tables st
          ON st.catalog_name = tt.catalog_name
          AND st.schema_name = tt.schema_name
          AND st.object_name = tt.table_name
        """,
    )
    column_tags = _run_query(
        token,
        f"""
        WITH {cte}
        SELECT
          ct.catalog_name,
          ct.schema_name,
          ct.table_name AS object_name,
          ct.column_name,
          ct.tag_name,
          ct.tag_value
        FROM system.information_schema.column_tags ct
        INNER JOIN selected_tables st
          ON st.catalog_name = ct.catalog_name
          AND st.schema_name = ct.schema_name
          AND st.object_name = ct.table_name
        """,
    )

    key = lambda row: (row["catalog_name"], row["schema_name"], row["object_name"])
    table_tag_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    for row in table_tags:
        table_tag_counts[key(row)] += 1

    column_count: dict[tuple[str, str, str], int] = defaultdict(int)
    columns_missing_desc: dict[tuple[str, str, str], int] = defaultdict(int)
    column_names: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    tagged_columns: dict[tuple[str, str, str], set[str]] = defaultdict(set)

    for row in columns:
        k = key(row)
        column_count[k] += 1
        column_names[k].add(row["column_name"])
        if not (row.get("comment") or "").strip():
            columns_missing_desc[k] += 1

    for row in column_tags:
        tagged_columns[key(row)].add(row["column_name"])

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = Path.cwd() / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        out_path.unlink()

    conn = sqlite3.connect(out_path)
    try:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO metadata_snapshot(
              created_at_utc, source, warehouse_id, table_limit, catalogs_filter
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                datetime.now(timezone.utc).isoformat(),
                "system.information_schema",
                _config().settings.sql_warehouse_id,
                args.table_limit,
                ",".join(catalogs),
            ),
        )

        for row in tables:
            k = key(row)
            tag_count = table_tag_counts.get(k, 0)
            col_total = column_count.get(k, 0)
            miss_desc = columns_missing_desc.get(k, 0)
            miss_tags = max(col_total - len(tagged_columns.get(k, set())), 0)
            table_desc_missing = int(not bool((row.get("comment") or "").strip()))
            table_owner_missing = int(not bool((row.get("owner") or "").strip()))
            table_tags_missing = int(tag_count == 0)
            total_gap_score = (
                table_desc_missing
                + table_owner_missing
                + table_tags_missing
                + miss_desc
                + miss_tags
            )
            conn.execute(
                """
                INSERT INTO metadata_objects(
                  catalog_name, schema_name, object_name, object_type, owner, comment,
                  table_tag_count, column_count, columns_missing_description, columns_missing_tags,
                  table_description_missing, table_owner_missing, table_tags_missing, total_gap_score
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["catalog_name"],
                    row["schema_name"],
                    row["object_name"],
                    row["object_type"],
                    row.get("owner"),
                    row.get("comment"),
                    tag_count,
                    col_total,
                    miss_desc,
                    miss_tags,
                    table_desc_missing,
                    table_owner_missing,
                    table_tags_missing,
                    total_gap_score,
                ),
            )

        conn.executemany(
            """
            INSERT INTO metadata_columns(
              catalog_name, schema_name, object_name, column_name, ordinal_position, data_type, comment
            ) VALUES (:catalog_name, :schema_name, :object_name, :column_name, :ordinal_position, :data_type, :comment)
            """,
            columns,
        )
        conn.executemany(
            """
            INSERT INTO metadata_table_tags(
              catalog_name, schema_name, object_name, tag_name, tag_value
            ) VALUES (:catalog_name, :schema_name, :object_name, :tag_name, :tag_value)
            """,
            table_tags,
        )
        conn.executemany(
            """
            INSERT INTO metadata_column_tags(
              catalog_name, schema_name, object_name, column_name, tag_name, tag_value
            ) VALUES (:catalog_name, :schema_name, :object_name, :column_name, :tag_name, :tag_value)
            """,
            column_tags,
        )
        conn.commit()
    finally:
        conn.close()

    print(f"SQLite cache built at: {out_path}")
    print(
        f"Loaded {len(tables)} objects, {len(columns)} columns, "
        f"{len(table_tags)} table tags, {len(column_tags)} column tags."
    )


if __name__ == "__main__":
    main()
