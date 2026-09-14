"""Row 5 — Where exactly does the failing table fail?

Same failing table as row 2. Hits every SQL-reachable surface separately:
DESCRIBE / DESCRIBE EXTENDED / DESCRIBE DETAIL / SELECT COUNT / SELECT * /
REFRESH FOREIGN TABLE / information_schema.columns / SHOW CREATE TABLE.

If ALL fail with the same class at table-resolution time, the defect is in
CatalogTable construction (before any read path). If information_schema still
lists the table, UC *knows* it exists but cannot materialize a schema for it —
useful to know for tooling that enumerates the catalog.
"""

from _common import load_config, record_result, section, workspace_client
from _matrix_common import identities, probe_uc_surfaces, write_and_inspect


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)
    section("row 05: failure surfaces on the list<bigint>/rest table")
    written = write_and_inspect(cfg, "rest", "list_primitive")
    surfaces = probe_uc_surfaces(cfg, written["table"], w)
    classes = {k: v.get("error_class") for k, v in surfaces.items() if not v.get("ok")}
    record_result(
        cfg,
        "05_failure_surfaces",
        {
            "question": "Does the failure differ across DESCRIBE / SELECT / REFRESH / information_schema?",
            "identity": identities(cfg, w),
            **written,
            "surfaces": surfaces,
            "failing_surfaces": classes,
            "single_error_class": len(set(classes.values())) <= 1,
            "status": "◑",
        },
    )


if __name__ == "__main__":
    main()
