import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _common import FQ_SCHEMA, count, detail, sql, summary, write_result


def main() -> None:
    source = f"{FQ_SCHEMA}.managed_delta_unregister_probe"
    sql(f"CREATE OR REPLACE TABLE {source} AS SELECT * FROM {FQ_SCHEMA}.managed_delta")
    location = str(detail(source).get("location", ""))
    unregister = sql(f"UNREGISTER TABLE {source}")
    reregister = (
        sql(f"CREATE TABLE {source} USING DELTA LOCATION '{location}'")
        if unregister.succeeded
        else None
    )
    recovered_rows = count(source) if reregister and reregister.succeeded else None
    status = "pass" if recovered_rows == 3 else "fail"
    write_result(
        "3",
        question="Can managed Delta be unregistered and re-registered as external without copying data?",
        status=status,
        finding=(
            "UNREGISTER followed by external registration preserved all rows without a data copy."
            if status == "pass"
            else "The no-copy unregister/re-register sequence was not supported end to end."
        ),
        evidence={
            "unregister": summary(unregister),
            "reregister": summary(reregister) if reregister else None,
            "recovered_rows": recovered_rows,
        },
    )
    print(f"row 3: {status}")


if __name__ == "__main__":
    main()
