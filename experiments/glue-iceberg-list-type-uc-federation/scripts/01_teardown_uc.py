"""Remove the Databricks-side federation objects (reverse of 00_setup_uc.py).

Also drops every experiment table from Glue via the REST catalog so the S3
bucket is empty-ish before `make tf-destroy` (force_destroy handles the rest).
"""

from _common import (
    SHAPES,
    WRITERS,
    dbsql_exec,
    iceberg_catalog,
    load_config,
    note,
    ok,
    section,
    table_name,
    workspace_client,
)


def _drop_uc(cfg, w) -> None:
    from databricks.sdk.errors import NotFound

    section("foreign catalog")
    try:
        dbsql_exec(cfg, f"DROP CATALOG IF EXISTS {cfg.uc_foreign_catalog} CASCADE", w)
        ok(f"dropped {cfg.uc_foreign_catalog}")
    except Exception as e:  # noqa: BLE001 — teardown reports, never aborts
        note(f"catalog: {e}")

    section("glue connection")
    try:
        w.connections.delete(cfg.uc_connection)
        ok(f"deleted {cfg.uc_connection}")
    except NotFound:
        note("absent")

    section("external location")
    try:
        w.external_locations.delete(cfg.uc_external_location, force=True)
        ok(f"deleted {cfg.uc_external_location}")
    except NotFound:
        note("absent")

    section("storage credential")
    try:
        w.storage_credentials.delete(cfg.uc_storage_credential, force=True)
        ok(f"deleted {cfg.uc_storage_credential}")
    except NotFound:
        note("absent")

    section("service credential")
    try:
        w.credentials.delete_credential(cfg.uc_service_credential, force=True)
        ok(f"deleted {cfg.uc_service_credential}")
    except NotFound:
        note("absent")


def _drop_glue_tables(cfg) -> None:
    from pyiceberg.exceptions import NoSuchTableError

    section("glue tables")
    cat = iceberg_catalog(cfg, "glue")
    for writer in WRITERS:
        for shape in SHAPES:
            ident = (cfg.glue_database, table_name(cfg, writer, shape))
            try:
                cat.drop_table(ident)
                ok(f"dropped {ident[1]}")
            except NoSuchTableError:
                note(f"absent {ident[1]}")


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)
    _drop_uc(cfg, w)
    _drop_glue_tables(cfg)
    print("\nteardown-uc: done (run `make tf-destroy` for the AWS side)")


if __name__ == "__main__":
    main()
