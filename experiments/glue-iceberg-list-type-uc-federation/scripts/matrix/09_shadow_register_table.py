"""Row 9 — Reader-side mirror: register a SHADOW Glue table over the same metadata.

The customer's writer is fixed (it commits through the Glue Iceberg REST
endpoint and stamps `list<>` into the Glue StorageDescriptor). Nothing on the
UC side can read that table (rows 5, 8). Can the *reader* side create a second
Glue table entry — via `GlueCatalog.register_table(<same metadata.json>)` —
that carries Hive-canonical `array<>` in its SD, and read *that* through UC?

Three questions, recorded separately:

  (a) Does UC read the shadow table at all?
  (b) After the real writer commits again, is the shadow table's SD still
      `array<>`? (It should be — the writer never touches the shadow entry.)
  (c) Does the shadow table go STALE after that commit? Its `metadata_location`
      still points at the old snapshot, so UC should read the old row count
      until the shadow is re-registered.
  (d) Re-register (drop + register at the new metadata_location): does UC now
      see the new rows?

(c)+(d) define the operational cost: a trigger (EventBridge on Glue UpdateTable,
or a poll) that re-registers the shadow on every source commit.
"""

from _common import (
    glue_sd_types,
    glue_table,
    iceberg_catalog,
    load_config,
    note,
    record_result,
    section,
    table_name,
    workspace_client,
)
from _matrix_common import identities, probe_uc_read, write_and_inspect


def glue_metadata_location(cfg, name: str) -> str:
    return glue_table(cfg, name)["Parameters"]["metadata_location"]


def register_shadow(cfg, shadow: str, metadata_location: str):
    """Drop (if present) and re-register the shadow table at `metadata_location`."""
    from pyiceberg.exceptions import NoSuchTableError

    cat = iceberg_catalog(cfg, "glue")
    ident = (cfg.glue_database, shadow)
    try:
        # purge=False: this entry does not own the files — the source table does.
        cat.drop_table(ident)
    except NoSuchTableError:
        pass
    return cat.register_table(ident, metadata_location)


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)
    section("row 09: shadow Glue table via register_table over the list<>/rest metadata")

    # Source: the reported-failure table, written through the REST endpoint.
    source = write_and_inspect(cfg, "rest", "list_primitive")
    source_name = source["table"]
    shadow_name = f"{table_name(cfg, 'rest', 'list_primitive')}_shadow"

    # (a) register + read
    meta_v1 = glue_metadata_location(cfg, source_name)
    register_shadow(cfg, shadow_name, meta_v1)
    shadow_sd_v1 = glue_sd_types(glue_table(cfg, shadow_name))
    note(f"shadow registered: sd_types={shadow_sd_v1}")
    read_v1 = probe_uc_read(cfg, shadow_name, w)
    count_v1 = _count(read_v1)
    note(f"UC read of shadow: readable={read_v1['readable']} count={count_v1}")

    # (b)+(c) the real writer commits again — does the shadow SD survive, and does it go stale?
    note("real writer performs one more REST-endpoint commit (append 3 rows)")
    source_after = write_and_inspect(cfg, "rest", "list_primitive")
    meta_v2 = glue_metadata_location(cfg, source_name)
    shadow_sd_after = glue_sd_types(glue_table(cfg, shadow_name))
    shadow_meta_after = glue_metadata_location(cfg, shadow_name)
    read_stale = probe_uc_read(cfg, shadow_name, w)
    count_stale = _count(read_stale)
    note(
        f"after source commit: shadow sd={shadow_sd_after} "
        f"shadow_meta_moved={shadow_meta_after != meta_v1} UC count={count_stale}"
    )

    # (d) re-register at the new metadata_location
    register_shadow(cfg, shadow_name, meta_v2)
    read_v2 = probe_uc_read(cfg, shadow_name, w)
    count_v2 = _count(read_v2)
    note(f"after re-register: UC readable={read_v2['readable']} count={count_v2}")

    # Source-of-truth count via Athena-free path: Iceberg metadata itself.
    cat = iceberg_catalog(cfg, "glue")
    shadow_tbl = cat.load_table((cfg.glue_database, shadow_name))
    true_rows = sum(int(f.file.record_count) for f in shadow_tbl.scan().plan_files())

    works = bool(read_v1["readable"])
    survives = shadow_sd_after == shadow_sd_v1 and "array<" in shadow_sd_after.get("list_ids", "")
    stale = (
        read_stale["readable"] and count_stale is not None and count_v2 is not None and count_stale < count_v2
    )
    fresh_after_reregister = read_v2["readable"] and count_v2 == true_rows

    record_result(
        cfg,
        "09_shadow_register_table",
        {
            "question": "Reader-side shadow Glue table (register_table over the same metadata.json) "
            "readable in UC? Survives source commits? Goes stale? Fresh after re-register?",
            "identity": identities(cfg, w),
            "source_table": source_name,
            "source_glue": source_after["glue"],
            "shadow_table": shadow_name,
            "shadow_sd_types_on_register": shadow_sd_v1,
            "shadow_sd_types_after_source_commit": shadow_sd_after,
            "uc_read_shadow_v1": read_v1,
            "uc_read_shadow_after_source_commit": read_stale,
            "uc_read_shadow_after_reregister": read_v2,
            "counts": {
                "shadow_v1": count_v1,
                "shadow_after_source_commit": count_stale,
                "shadow_after_reregister": count_v2,
                "iceberg_metadata_truth": true_rows,
            },
            "shadow_readable_in_uc": works,
            "shadow_sd_survives_source_commit": survives,
            "shadow_goes_stale_after_source_commit": stale,
            "shadow_fresh_after_reregister": fresh_after_reregister,
            "status": "◑" if works and stale else ("✅" if works else "❌"),
        },
    )


def _count(read: dict):
    sample = read.get("select_count", {}).get("sample")
    try:
        return int(sample[0][0])
    except (TypeError, IndexError, ValueError):
        return None


if __name__ == "__main__":
    main()
