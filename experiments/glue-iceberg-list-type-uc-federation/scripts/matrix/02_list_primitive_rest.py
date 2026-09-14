"""Row 2 — The reported failure: list<bigint> via the Glue REST endpoint.

Hypothesis: the REST endpoint writes Iceberg's `list<bigint>` token into the
Glue StorageDescriptor; UC federation parses SD Hive types BEFORE the Iceberg
metadata.json path and rejects `list` with
INTERNAL_ERROR_INVALID_HIVE_COLUMN_TYPE.

Two things are recorded: what Glue's SD actually says (the cause) and what UC
does with it (the effect). They are separable findings.
"""

from _common import load_config, record_result, section, workspace_client
from _matrix_common import identities, probe_uc_read, write_and_inspect

EXPECTED_ERROR = "INTERNAL_ERROR_INVALID_HIVE_COLUMN_TYPE"


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)
    section("row 02: list<bigint> via glue-rest")
    written = write_and_inspect(cfg, "rest", "list_primitive")
    sd_type = written["glue"]["sd_types"].get("list_ids")
    read = probe_uc_read(cfg, written["table"], w)
    got = read["describe"].get("error_class") or read["select_count"].get("error_class")
    record_result(
        cfg,
        "02_list_primitive_rest",
        {
            "question": "Same writer, list<bigint> column -> "
            "INTERNAL_ERROR_INVALID_HIVE_COLUMN_TYPE reproduced?",
            "writer": "rest",
            "shape": "list_primitive",
            "identity": identities(cfg, w),
            **written,
            "sd_type_of_list_column": sd_type,
            "sd_uses_iceberg_list_token": bool(sd_type and sd_type.startswith("list<")),
            "uc": read,
            "observed_error_class": got,
            "reproduced": (not read["readable"]) and got == EXPECTED_ERROR,
            "status": "✅" if read["readable"] else "❌",
        },
    )


if __name__ == "__main__":
    main()
