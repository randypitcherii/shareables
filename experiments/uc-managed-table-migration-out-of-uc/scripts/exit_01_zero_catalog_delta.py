"""Exit 1: read the managed table's Delta log from S3 with no catalog at all."""

from _common import AWS_REGION, SOURCES, source_locations, write_result
from exits import read_delta_with_deltalake, read_delta_with_duckdb

ROW = {"managed_delta": "1a", "managed_iceberg": "1b"}


def main() -> None:
    for name, location in source_locations().items():
        deltalake_read = read_delta_with_deltalake(location)
        duckdb_read = read_delta_with_duckdb(location, region=AWS_REGION)
        passing = [c for c in (deltalake_read, duckdb_read) if c.succeeded and c.rows == 3]
        if len(passing) == 2:
            status = "pass"
        elif passing:
            status = "partial"
        else:
            status = "fail"
        finding = {
            "pass": "Both Python Delta clients read all rows directly from the managed path.",
            "partial": (
                f"{passing[0].client} read all rows straight from the managed path with no "
                "catalog. The other client refused the table's Delta protocol features, so the "
                "storage exit works but engine support for the default protocol varies."
            ),
            "fail": "No catalog-free Python Delta client could read the managed path.",
        }[status]
        write_result(
            ROW[name],
            question=(
                f"{SOURCES[name]} → zero-catalog Delta on S3: can a Python client read the "
                "existing files with no data copy and no catalog?"
            ),
            status=status,
            finding=finding,
            evidence={
                "source": name,
                "data_copied": False,
                "path_inside_uc_managed_prefix": "__unitystorage" in location,
                "clients": [deltalake_read.as_dict(), duckdb_read.as_dict()],
            },
        )
        print(f"row {ROW[name]}: {status}")


if __name__ == "__main__":
    main()
