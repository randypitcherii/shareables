"""Row 6 — The suggested workaround: patch the Glue SD list<> -> array<>.

Two questions, answered in order:
  (a) After `glue:UpdateTable` rewrites list_ids to array<bigint>, does UC read
      the table? (Does the workaround work AT ALL?)
  (b) After the REST-endpoint writer performs its next commit (one append),
      does the SD revert to list<bigint>, and does UC break again?

(b) is the load-bearing one: a streaming writer commits continuously, so a
workaround that survives zero commits is not a workaround.
"""

import copy
import time

from _common import (
    boto_session,
    ensure_table,
    glue_sd_types,
    glue_table,
    load_config,
    note,
    record_result,
    section,
    workspace_client,
)
from _matrix_common import identities, probe_uc_read, write_and_inspect

_TABLE_INPUT_KEYS = (
    "Name", "Description", "Owner", "Retention", "StorageDescriptor", "PartitionKeys",
    "ViewOriginalText", "ViewExpandedText", "TableType", "Parameters", "TargetTable",
)


def patch_list_to_array(cfg, name: str) -> dict[str, str]:
    glue = boto_session(cfg).client("glue")
    table = glue.get_table(DatabaseName=cfg.glue_database, Name=name)["Table"]
    table_input = {k: copy.deepcopy(v) for k, v in table.items() if k in _TABLE_INPUT_KEYS}
    changed = {}
    for col in table_input.get("StorageDescriptor", {}).get("Columns", []):
        if col["Type"].startswith("list<"):
            new = "array<" + col["Type"][len("list<"):]
            changed[col["Name"]] = f"{col['Type']} -> {new}"
            col["Type"] = new
    glue.update_table(DatabaseName=cfg.glue_database, TableInput=table_input)
    return changed


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)
    section("row 06: SD patch list<> -> array<>, then a writer commit")
    written = write_and_inspect(cfg, "rest", "list_primitive")
    name = written["table"]
    before = probe_uc_read(cfg, name, w)

    changed = patch_list_to_array(cfg, name)
    note(f"patched: {changed}")
    sd_after_patch = glue_sd_types(glue_table(cfg, name))
    time.sleep(5)
    after_patch = probe_uc_read(cfg, name, w)

    note("performing one more REST-endpoint commit (append)")
    ensure_table(cfg, "rest", "list_primitive", rows=1)
    sd_after_commit = glue_sd_types(glue_table(cfg, name))
    time.sleep(5)
    after_commit = probe_uc_read(cfg, name, w)

    record_result(
        cfg,
        "06_sd_patch_durability",
        {
            "question": "Manual SD patch list<> -> array<>: UC reads? "
            "Survives the next REST-endpoint commit?",
            "identity": identities(cfg, w),
            **written,
            "before_patch": {"readable": before["readable"]},
            "patch": changed,
            "after_patch": {"sd_types": sd_after_patch, "readable": after_patch["readable"],
                            "error_class": after_patch["describe"].get("error_class")},
            "after_next_commit": {"sd_types": sd_after_commit, "readable": after_commit["readable"],
                                  "error_class": after_commit["describe"].get("error_class")},
            "workaround_works_once": after_patch["readable"],
            "workaround_survives_commit": after_commit["readable"]
            and sd_after_commit.get("list_ids", "").startswith("array<"),
            "status": "✅" if after_commit["readable"] else ("◑" if after_patch["readable"] else "❌"),
        },
    )


if __name__ == "__main__":
    main()
