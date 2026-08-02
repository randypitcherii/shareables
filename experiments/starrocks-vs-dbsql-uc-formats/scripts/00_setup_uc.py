"""Create the evaluation schema and grant EXTERNAL USE SCHEMA to the operator.

EXTERNAL USE SCHEMA is what lets the UC Iceberg REST catalog vend storage
credentials to an external engine (StarRocks) for tables in this schema. The
metastore must also have external data access enabled (workspace admin setting).
"""

from _common import dbsql_exec, load_config, section, workspace_client


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)

    section("current identity")
    rows, _ = dbsql_exec(cfg, "SELECT current_user()", w)
    user = rows[0][0]
    print(f"operator: {user}")

    section(f"schema {cfg.uc_catalog}.{cfg.uc_schema}")
    dbsql_exec(cfg, f"CREATE SCHEMA IF NOT EXISTS {cfg.uc_catalog}.{cfg.uc_schema}", w)
    print("schema ready")

    section("grant EXTERNAL USE SCHEMA")
    dbsql_exec(
        cfg,
        f"GRANT EXTERNAL USE SCHEMA ON SCHEMA {cfg.uc_catalog}.{cfg.uc_schema} TO `{user}`",
        w,
    )
    print(f"granted to {user}")


if __name__ == "__main__":
    main()
