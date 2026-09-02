import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _common import FQ_SCHEMA, count, detail, sql, summary, write_result


def main() -> None:
    source = f"{FQ_SCHEMA}.managed_iceberg"
    source_probe = sql(f"SELECT count(*) FROM {source}")
    source_detail = detail(source) if source_probe.succeeded else {}
    source_rows = count(source) if source_probe.succeeded else None
    write_result(
        "4",
        question="Can managed Iceberg be migrated to a non-Databricks Iceberg catalog?",
        status="inconclusive",
        finding=(
            "The run has no independent destination catalog configured; no portability verdict is asserted."
        ),
        evidence={
            "source_probe": summary(source_probe),
            "source_format": source_detail.get("format"),
            "source_rows": source_rows,
            "destination_catalog_configured": False,
        },
    )
    print("row 4: inconclusive")


if __name__ == "__main__":
    main()
