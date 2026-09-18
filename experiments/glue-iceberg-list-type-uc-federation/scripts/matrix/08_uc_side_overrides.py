"""Row 8 — Any UC-side lever that sidesteps SD parsing? (Expected: ❌/❓.)

Attempts, each recorded with its verbatim error class:
  - REFRESH FOREIGN TABLE ... (plain)
  - CREATE VIEW over the failing table (does a view defer resolution? no — but
    the error class tells us where resolution happens)
  - CREATE TABLE ... USING iceberg LOCATION '<metadata.json path>' in a
    regular UC catalog — the "bypass Glue entirely, point at metadata" route.
    Needs UC_BYPASS_CATALOG/UC_BYPASS_SCHEMA set to a writable catalog.schema.
  - read_files() over the table's parquet data path (format => 'parquet') —
    not Iceberg-aware, but proves storage + credentials are fine and the
    failure is purely metadata-layer.
"""

import os

from _common import glue_table, load_config, note, probe_sql, record_result, section, workspace_client
from _matrix_common import fq, identities, write_and_inspect


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)
    section("row 08: UC-side overrides against the list<bigint>/rest table")
    written = write_and_inspect(cfg, "rest", "list_primitive")
    name = written["table"]
    target = fq(cfg, name)
    raw = glue_table(cfg, name)
    metadata_location = raw.get("Parameters", {}).get("metadata_location", "")
    data_path = f"{cfg.warehouse_path}/{cfg.glue_database}/{name}/data"

    attempts = {
        "refresh_foreign_table": probe_sql(cfg, f"REFRESH FOREIGN TABLE {target}", w),
        "read_files_parquet": probe_sql(
            cfg, f"SELECT COUNT(*) FROM read_files('{data_path}', format => 'parquet')", w
        ),
    }

    bypass_cat = os.environ.get("UC_BYPASS_CATALOG")
    bypass_sch = os.environ.get("UC_BYPASS_SCHEMA")
    if bypass_cat and bypass_sch and metadata_location:
        probe_sql(cfg, f"CREATE SCHEMA IF NOT EXISTS `{bypass_cat}`.`{bypass_sch}`", w)
        ext = f"`{bypass_cat}`.`{bypass_sch}`.`{name}_bypass`"
        attempts["create_external_iceberg_at_metadata"] = probe_sql(
            cfg,
            f"CREATE TABLE IF NOT EXISTS {ext} USING iceberg LOCATION '{metadata_location}'",
            w,
        )
        attempts["select_bypass"] = probe_sql(cfg, f"SELECT COUNT(*) FROM {ext}", w)
        probe_sql(cfg, f"DROP TABLE IF EXISTS {ext}", w)
        view = f"`{bypass_cat}`.`{bypass_sch}`.`{name}_view`"
        attempts["create_view_over_foreign"] = probe_sql(
            cfg, f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM {target}", w
        )
        probe_sql(cfg, f"DROP VIEW IF EXISTS {view}", w)
    else:
        note("UC_BYPASS_CATALOG/UC_BYPASS_SCHEMA unset — skipping create-table/view attempts")

    for k, v in attempts.items():
        note(f"{k}: ok={v['ok']} {v.get('error_class') or ''}")
    any_ok = any(v["ok"] for k, v in attempts.items() if k != "read_files_parquet")
    record_result(
        cfg,
        "08_uc_side_overrides",
        {
            "question": "Any UC-side schema override that sidesteps SD parsing?",
            "identity": identities(cfg, w),
            **written,
            "metadata_location_present": bool(metadata_location),
            "attempts": {k: {kk: vv for kk, vv in v.items() if kk != "rows"} for k, v in attempts.items()},
            "storage_reachable_via_read_files": attempts["read_files_parquet"]["ok"],
            "status": "✅" if any_ok else "❌",
        },
    )


if __name__ == "__main__":
    main()
