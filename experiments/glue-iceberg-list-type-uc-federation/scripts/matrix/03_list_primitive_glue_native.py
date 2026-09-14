"""Row 3 — Same shape, different writer: pyiceberg GlueCatalog (client-side SD).

The Iceberg-native Glue integration (Java IcebergToGlueConverter, and
pyiceberg's mirror of it) maps LIST -> `array<...>`. If the SD reads
`array<bigint>` and UC reads the table, the defect is scoped to the
REST-endpoint writer path — which tells the customer exactly which knob is
theirs and which is not.
"""

from _common import load_config, record_result, section, workspace_client
from _matrix_common import identities, probe_uc_read, write_and_inspect


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)
    section("row 03: list<bigint> via pyiceberg GlueCatalog")
    written = write_and_inspect(cfg, "glue", "list_primitive")
    sd_type = written["glue"]["sd_types"].get("list_ids")
    read = probe_uc_read(cfg, written["table"], w)
    record_result(
        cfg,
        "03_list_primitive_glue_native",
        {
            "question": "Same table shape via native Iceberg GlueCatalog -> SD gets array<> and UC reads it?",
            "writer": "glue",
            "shape": "list_primitive",
            "identity": identities(cfg, w),
            **written,
            "sd_type_of_list_column": sd_type,
            "sd_uses_hive_array_token": bool(sd_type and sd_type.startswith("array<")),
            "uc": read,
            "status": "✅" if read["readable"] else "❌",
        },
    )


if __name__ == "__main__":
    main()
