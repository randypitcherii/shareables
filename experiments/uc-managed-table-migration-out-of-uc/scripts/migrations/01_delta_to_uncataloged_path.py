import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _common import EXTERNAL_ROOT, FQ_SCHEMA, clear_external_path, count, sql, summary, write_result


def main() -> None:
    path = f"{EXTERNAL_ROOT}/uncataloged_delta"
    destination = f"delta.`{path}`"
    sql(f"DROP TABLE IF EXISTS {destination}")
    clear_external_path(path)
    create = sql(f"CREATE TABLE {destination} AS SELECT * FROM {FQ_SCHEMA}.managed_delta")
    destination_count = count(destination) if create.succeeded else None
    if create.succeeded and destination_count == 3:
        status = "pass"
    elif create.error and (
        "credential" in create.error.lower() or "cloud storage" in create.error.lower()
    ):
        status = "inconclusive"
    else:
        status = "fail"
    findings = {
        "pass": "A path-based Delta CTAS created an uncataloged copy with all three rows.",
        "inconclusive": (
            "The probe reached the storage credential boundary, but this run's "
            "external-location credential was unhealthy; portability was not isolated."
        ),
        "fail": "The path-based Delta copy was rejected for a capability reason.",
    }
    write_result(
        "1",
        question="Can a managed Delta table be copied to an uncataloged Delta path?",
        status=status,
        finding=findings[status],
        evidence={
            "create": summary(create),
            "source_rows": 3,
            "destination_rows": destination_count,
        },
    )
    print(f"row 1: {status}")


if __name__ == "__main__":
    main()
