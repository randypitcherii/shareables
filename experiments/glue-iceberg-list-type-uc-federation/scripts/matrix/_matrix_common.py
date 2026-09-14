"""The one loop every matrix row runs: write → inspect Glue SD → probe UC.

Each helper returns plain dicts so the numbered scripts stay thin and the
results JSON stays uniform.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import (  # noqa: E402
    Config,
    aws_identity,
    dbsql_identity,
    ensure_table,
    glue_table,
    glue_table_summary,
    note,
    probe_sql,
    table_name,
)

# How long to wait for UC to notice a brand-new Glue table. Federation syncs on
# interaction; a short retry loop keeps a slow sync from masquerading as a
# type failure.
SYNC_RETRIES = 6
SYNC_SLEEP_S = 10


def fq(cfg: Config, name: str) -> str:
    return f"`{cfg.uc_foreign_catalog}`.`{cfg.glue_database}`.`{name}`"


def write_and_inspect(cfg: Config, writer: str, shape: str, rows: int = 3, replace: bool = False) -> dict:
    """Write one (writer, shape) table and return what Glue now says about it."""
    ensure_table(cfg, writer, shape, rows=rows, replace=replace)
    name = table_name(cfg, writer, shape)
    summary = glue_table_summary(glue_table(cfg, name))
    note(f"{name}: sd_types={summary['sd_types']}")
    return {"table": name, "glue": summary}


def probe_uc_read(cfg: Config, name: str, w=None, retries: int = SYNC_RETRIES) -> dict:
    """DESCRIBE + SELECT COUNT(*) against the foreign table, with sync retries.

    A TABLE_OR_VIEW_NOT_FOUND on the first attempts is treated as "not synced
    yet" and retried; any other error is returned immediately as the finding.
    """
    target = fq(cfg, name)
    describe = None
    for attempt in range(retries):
        describe = probe_sql(cfg, f"DESCRIBE TABLE {target}", w)
        if describe["ok"] or describe.get("error_class") not in ("TABLE_OR_VIEW_NOT_FOUND",):
            break
        note(f"not synced yet (attempt {attempt + 1}/{retries}); sleeping {SYNC_SLEEP_S}s")
        time.sleep(SYNC_SLEEP_S)
    select = probe_sql(cfg, f"SELECT COUNT(*) FROM {target}", w)
    return {
        "describe": _slim(describe),
        "select_count": _slim(select),
        "readable": bool(describe["ok"] and select["ok"]),
    }


def probe_uc_surfaces(cfg: Config, name: str, w=None) -> dict:
    """Every read surface we can hit from SQL, each recorded separately."""
    target = fq(cfg, name)
    surfaces = {
        "describe": f"DESCRIBE TABLE {target}",
        "describe_extended": f"DESCRIBE TABLE EXTENDED {target}",
        "describe_detail": f"DESCRIBE DETAIL {target}",
        "select_count": f"SELECT COUNT(*) FROM {target}",
        "select_star_limit": f"SELECT * FROM {target} LIMIT 1",
        "refresh_foreign_table": f"REFRESH FOREIGN TABLE {target}",
        "information_schema_columns": (
            f"SELECT column_name, data_type FROM `{cfg.uc_foreign_catalog}`.information_schema.columns "
            f"WHERE table_schema = '{cfg.glue_database}' AND table_name = '{name}'"
        ),
        "show_create_table": f"SHOW CREATE TABLE {target}",
    }
    return {k: _slim(probe_sql(cfg, sql, w)) for k, sql in surfaces.items()}


def identities(cfg: Config, w=None) -> dict:
    return {"databricks": dbsql_identity(cfg, w), "aws": aws_identity(cfg)}


def _slim(outcome: dict) -> dict:
    """Drop bulky row payloads; keep the verdict and the error class/text."""
    keep = {k: v for k, v in outcome.items() if k not in ("rows",)}
    if outcome.get("ok") and outcome.get("rows"):
        keep["sample"] = outcome["rows"][:5]
    return keep


def verdict(readable: bool, expected_readable: bool) -> str:
    """✅ when the observation matches the doc/claim, ❌ when it does not."""
    return "✅" if readable == expected_readable else "❌"
