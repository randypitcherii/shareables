"""Create the Databricks-side federation objects from the terraform outputs.

Idempotent. Each object is created only if it does not already exist:

  1. service credential   -> the self-assuming IAM role (Glue API access)
  2. storage credential   -> the SAME role (S3 access)
  3. external location    -> s3://<bucket>/  (covers warehouse + uc-metadata)
  4. Glue connection      -> CREATE CONNECTION ... TYPE glue
  5. foreign catalog      -> CREATE FOREIGN CATALOG ... storage_root (required
                            for Iceberg reads — inheriting is NOT sufficient)

Requires CREATE SERVICE CREDENTIAL / CREATE STORAGE CREDENTIAL / CREATE
CONNECTION / CREATE CATALOG on the metastore (metastore admin has all four).

Teardown: `make teardown-uc` (01_teardown_uc.py) removes them in reverse order.
"""

from _common import (
    dbsql_exec,
    dbsql_identity,
    load_config,
    note,
    ok,
    section,
    workspace_client,
)


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)
    from databricks.sdk.errors import NotFound
    from databricks.sdk.service.catalog import AwsIamRole, AwsIamRoleRequest

    section("identity")
    ok(f"databricks: {dbsql_identity(cfg, w)}")

    section("service credential")
    try:
        w.credentials.get_credential(cfg.uc_service_credential)
        note(f"exists: {cfg.uc_service_credential}")
    except NotFound:
        w.credentials.create_credential(
            name=cfg.uc_service_credential,
            purpose="SERVICE",
            aws_iam_role=AwsIamRole(role_arn=cfg.uc_federation_role_arn),
            comment="glue-iceberg-list-type-uc-federation experiment (ephemeral)",
        )
        ok(f"created: {cfg.uc_service_credential}")
    cred = w.credentials.get_credential(cfg.uc_service_credential)
    note(f"external_id={cred.aws_iam_role.external_id if cred.aws_iam_role else None}")

    section("storage credential")
    try:
        w.storage_credentials.get(cfg.uc_storage_credential)
        note(f"exists: {cfg.uc_storage_credential}")
    except NotFound:
        w.storage_credentials.create(
            name=cfg.uc_storage_credential,
            aws_iam_role=AwsIamRoleRequest(role_arn=cfg.uc_federation_role_arn),
            comment="glue-iceberg-list-type-uc-federation experiment (ephemeral)",
        )
        ok(f"created: {cfg.uc_storage_credential}")

    section("external location")
    try:
        w.external_locations.get(cfg.uc_external_location)
        note(f"exists: {cfg.uc_external_location}")
    except NotFound:
        w.external_locations.create(
            name=cfg.uc_external_location,
            url=f"s3://{cfg.s3_bucket}/",
            credential_name=cfg.uc_storage_credential,
            comment="glue-iceberg-list-type-uc-federation experiment (ephemeral)",
        )
        ok(f"created: {cfg.uc_external_location} -> s3://<bucket>/")

    section("glue connection")
    try:
        w.connections.get(cfg.uc_connection)
        note(f"exists: {cfg.uc_connection}")
    except NotFound:
        dbsql_exec(
            cfg,
            f"CREATE CONNECTION {cfg.uc_connection} TYPE glue OPTIONS ("
            f"aws_region '{cfg.aws_region}', "
            f"aws_account_id '{cfg.aws_account_id}', "
            f"credential '{cfg.uc_service_credential}')",
            w,
        )
        ok(f"created: {cfg.uc_connection}")

    section("foreign catalog")
    try:
        w.catalogs.get(cfg.uc_foreign_catalog)
        note(f"exists: {cfg.uc_foreign_catalog}")
    except NotFound:
        dbsql_exec(
            cfg,
            f"CREATE FOREIGN CATALOG {cfg.uc_foreign_catalog} "
            f"USING CONNECTION {cfg.uc_connection} OPTIONS ("
            f"authorized_paths '{cfg.warehouse_path}', "
            f"storage_root '{cfg.uc_metadata_root}')",
            w,
        )
        ok(f"created: {cfg.uc_foreign_catalog} (storage_root set — required for Iceberg)")

    section("populate")
    rows = dbsql_exec(cfg, f"SHOW SCHEMAS IN {cfg.uc_foreign_catalog}", w)
    note(f"schemas visible: {[r[0] for r in rows]}")
    print("\nsetup-uc: done")


if __name__ == "__main__":
    main()
