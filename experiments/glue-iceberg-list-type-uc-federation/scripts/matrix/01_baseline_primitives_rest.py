"""Row 1 — Baseline: primitives-only Iceberg table via the Glue REST endpoint.

Claim (docs): Glue federation reads Iceberg tables (Public Preview) when the
foreign catalog has a storage_root. If THIS fails, nothing downstream is a
type finding — it is a setup finding.
"""

from _common import load_config, record_result, section, workspace_client
from _matrix_common import identities, probe_uc_read, write_and_inspect


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)
    section("row 01: primitives via glue-rest")
    written = write_and_inspect(cfg, "rest", "primitives")
    read = probe_uc_read(cfg, written["table"], w)
    record_result(
        cfg,
        "01_baseline_primitives_rest",
        {
            "question": "Primitives-only Iceberg table written via Glue Iceberg REST endpoint "
            "is readable in UC?",
            "writer": "rest",
            "shape": "primitives",
            "identity": identities(cfg, w),
            **written,
            "uc": read,
            "status": "✅" if read["readable"] else "❌",
        },
    )


if __name__ == "__main__":
    main()
