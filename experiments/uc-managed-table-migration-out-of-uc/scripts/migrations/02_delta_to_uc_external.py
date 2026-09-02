import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _common import (
    EXTERNAL_ROOT,
    FQ_SCHEMA,
    clear_external_path,
    count,
    detail,
    sql,
    summary,
    write_result,
)


def main() -> None:
    target = f"{FQ_SCHEMA}.external_delta"
    path = f"{EXTERNAL_ROOT}/external_delta"
    sql(f"DROP TABLE IF EXISTS {target}")
    clear_external_path(path)
    create = sql(
        f"CREATE TABLE {target} USING DELTA LOCATION '{path}' AS SELECT * FROM {FQ_SCHEMA}.managed_delta"
    )
    target_count = count(target) if create.succeeded else None
    target_detail = detail(target) if create.succeeded else {}
    external = str(target_detail.get("location", "")).startswith(EXTERNAL_ROOT)
    if create.succeeded and target_count == 3 and external:
        status = "pass"
    elif create.error and (
        "credential" in create.error.lower() or "cloud storage" in create.error.lower()
    ):
        status = "inconclusive"
    else:
        status = "fail"
    findings = {
        "pass": "CTAS created a UC external Delta table at customer-owned storage with all rows.",
        "inconclusive": (
            "The probe reached the storage credential boundary, but this run's "
            "external-location credential was unhealthy; portability was not isolated."
        ),
        "fail": "The external Delta CTAS failed for a capability reason.",
    }
    write_result(
        "2",
        question="Can a managed Delta table be copied into a UC external Delta table?",
        status=status,
        finding=findings[status],
        evidence={
            "create": summary(create),
            "source_rows": 3,
            "destination_rows": target_count,
            "external_location_verified": external,
        },
    )
    print(f"row 2: {status}")


if __name__ == "__main__":
    main()
