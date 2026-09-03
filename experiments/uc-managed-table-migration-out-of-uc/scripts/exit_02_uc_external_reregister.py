"""Exit 2: re-register the managed table's existing files as a UC external Delta table.

Three attempts per source, each without copying data:
  a) CREATE TABLE ... LOCATION over the live managed path
  b) DROP the managed table, then CREATE TABLE ... LOCATION over the same path
  c) control: the same statement over a byte-for-byte copy placed outside the
     reserved `__unitystorage` prefix (proves the refusal is about the prefix,
     not the files). The control copies data and is not itself an exit path.
"""

import re
import subprocess

from _common import (
    AWS_PROFILE,
    AWS_REGION,
    EXTERNAL_ROOT,
    EXTERNAL_TABLE_SCHEMA,
    FQ_SCHEMA,
    MANAGED_CATALOG,
    SOURCES,
    count,
    detail,
    must_sql,
    sql,
    summary,
    write_result,
)

ROW = {"managed_delta": "2a", "managed_iceberg": "2b"}
FQ_EXTERNAL = f"`{MANAGED_CATALOG}`.`{EXTERNAL_TABLE_SCHEMA}`"


def tblproperties_from_error(error: str | None) -> str:
    """UC requires CREATE ... LOCATION to restate the existing Delta log's properties.

    It tells you which ones in DELTA_CREATE_TABLE_WITH_DIFFERENT_PROPERTY; parse the
    `== Existing ==` block and render a TBLPROPERTIES clause.
    """
    if not error or "== Existing ==" not in error:
        return ""
    block = error.split("== Existing ==", 1)[1]
    pairs = re.findall(r"^\s*([\w.-]+)=(\S+)\s*$", block, flags=re.MULTILINE)
    if not pairs:
        return ""
    inner = ", ".join(f"'{k}'='{v}'" for k, v in sorted(pairs))
    return f" TBLPROPERTIES ({inner})"


def register(table: str, location: str):
    """CREATE ... LOCATION, retrying once with the properties UC says the log already has."""
    first = sql(f"CREATE TABLE {table} USING DELTA LOCATION '{location}'")
    if first.succeeded or "DIFFERENT_PROPERTY" not in (first.error or ""):
        return first
    props = tblproperties_from_error(first.error)
    return sql(f"CREATE TABLE {table} USING DELTA LOCATION '{location}'{props}")


def s3_copy(src: str, dst: str) -> None:
    cmd = ["aws", "s3", "sync", f"{src}/", f"{dst}/", "--quiet"]
    if AWS_PROFILE:
        cmd += ["--profile", AWS_PROFILE]
    cmd += ["--region", AWS_REGION]
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def main() -> None:
    must_sql(f"CREATE SCHEMA IF NOT EXISTS {FQ_EXTERNAL}")
    for name in SOURCES:
        source = f"{FQ_SCHEMA}.{name}"
        location = str(detail(source)["location"])

        live = register(f"{FQ_EXTERNAL}.{name}_live", location)
        live_rows = count(f"{FQ_EXTERNAL}.{name}_live") if live.succeeded else None

        must_sql(f"DROP TABLE {source}")
        after_drop = register(f"{FQ_EXTERNAL}.{name}_after_drop", location)
        after_drop_rows = (
            count(f"{FQ_EXTERNAL}.{name}_after_drop") if after_drop.succeeded else None
        )

        control_path = f"{EXTERNAL_ROOT}/control-copies/{name}"
        s3_copy(location, control_path)
        control = register(f"{FQ_EXTERNAL}.{name}_control", control_path)
        control_rows = count(f"{FQ_EXTERNAL}.{name}_control") if control.succeeded else None

        no_copy_worked = 3 in (live_rows, after_drop_rows)
        overlap = any(
            r.error and "LOCATION_OVERLAP" in r.error for r in (live, after_drop) if not r.succeeded
        )
        status = "pass" if no_copy_worked else "fail"
        control_note = (
            f" The identical files copied outside that prefix registered and read {control_rows} "
            "rows, so the block is the prefix rule, not the Delta log."
            if control_rows == 3
            else " The control copy outside that prefix also failed; see evidence."
        )
        if no_copy_worked:
            finding = "UC registered the existing managed files as an external Delta table without a copy."
        elif overlap:
            finding = (
                "UC refused every no-copy registration with LOCATION_OVERLAP: paths under the "
                "reserved `__unitystorage` prefix cannot become external tables, even after the "
                "managed table is dropped." + control_note
            )
        else:
            finding = "No-copy registration failed for a reason other than LOCATION_OVERLAP."
        write_result(
            ROW[name],
            question=(
                f"{SOURCES[name]} → UC external Delta table: can the existing files be "
                "re-registered in UC without a data copy?"
            ),
            status=status,
            finding=finding,
            evidence={
                "source": name,
                "data_copied_for_exit": False,
                "register_over_live_managed_path": summary(live),
                "register_after_dropping_managed_table": summary(after_drop),
                "control_copy_outside_unitystorage_prefix": {
                    "data_copied": True,
                    "result": summary(control),
                    "rows": control_rows,
                },
            },
        )
        print(f"row {ROW[name]}: {status}")


if __name__ == "__main__":
    main()
