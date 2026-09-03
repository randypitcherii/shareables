"""Exit 3: register the managed table's Iceberg metadata in a brand-new non-UC catalog.

UniForm-enabled managed Delta writes Iceberg metadata under `metadata/`;
managed Iceberg writes it under `_iceberg/metadata/`. Either way the newest
metadata.json is handed to a pyiceberg SQLite catalog, read, and then appended
to through that catalog so the new catalog owns the table's history.
"""

from _common import AWS_REGION, SOURCES, source_locations, write_result
from exits import latest_iceberg_metadata, list_s3_keys, register_iceberg_in_new_catalog

ROW = {"managed_delta": "3a", "managed_iceberg": "3b"}


def main() -> None:
    for name, location in source_locations().items():
        keys = list_s3_keys(location, region=AWS_REGION)
        relative_metadata = latest_iceberg_metadata(keys)
        if relative_metadata is None:
            write_result(
                ROW[name],
                question=(
                    f"{SOURCES[name]} → Iceberg in a new managing catalog: can existing "
                    "metadata be adopted with no data copy?"
                ),
                status="fail",
                finding="No Iceberg metadata was present alongside the Delta log.",
                evidence={"source": name, "keys_sampled": sorted(keys)[:20]},
            )
            print(f"row {ROW[name]}: fail")
            continue
        outcome = register_iceberg_in_new_catalog(
            f"{location}/{relative_metadata}",
            append_row={"id": 4, "value": "written-by-new-catalog"},
        )
        detail = outcome.detail or {}
        status = (
            "pass"
            if outcome.succeeded and detail.get("rows_at_registration") == 3 and outcome.rows == 4
            else "fail"
        )
        write_result(
            ROW[name],
            question=(
                f"{SOURCES[name]} → Iceberg in a new managing catalog: can existing metadata "
                "be adopted with no data copy?"
            ),
            status=status,
            finding=(
                "A fresh pyiceberg catalog adopted the existing metadata.json, read all rows, and "
                "committed a new snapshot in place. No data was copied; the new catalog now "
                "manages the table."
                if status == "pass"
                else "The new catalog could not adopt and advance the existing Iceberg metadata."
            ),
            evidence={
                "source": name,
                "data_copied": False,
                "iceberg_metadata_dir": relative_metadata.rsplit("/", 1)[0],
                "client": outcome.as_dict(),
            },
        )
        print(f"row {ROW[name]}: {status}")


if __name__ == "__main__":
    main()
