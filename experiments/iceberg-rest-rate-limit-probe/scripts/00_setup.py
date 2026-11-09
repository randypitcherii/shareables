"""Create the probe's Unity Catalog objects (idempotent).

- schema UC_SCHEMA in UC_CATALOG
- PROBE_TABLE_COUNT tiny managed Iceberg tables t000..tNNN (spread-load targets; t000 is
  also the single "hot" table)
- one UniForm Delta table `uniform_t000` for the legacy read-only /iceberg endpoint
- volume `probe_logs` for uploading raw per-call logs
- EXTERNAL USE SCHEMA for the calling principal (required for Iceberg REST access,
  even for the owner)

Runs for workspace A and, if DATABRICKS_PROFILE_B is set, workspace B: each workspace
gets the same objects in its own catalog (UC_CATALOG / UC_CATALOG_B).
"""

from concurrent.futures import ThreadPoolExecutor

from _common import load_config, run_sql, workspace_client


def main() -> None:
    cfg = load_config()
    for label in cfg.labels():
        setup(cfg, label)


def setup(cfg, prof: str) -> None:
    me = workspace_client(cfg.profile(prof)).current_user.me().user_name
    fq = f"`{cfg.catalog(prof)}`.`{cfg.uc_schema}`"
    print(f"== workspace {prof}")

    run_sql(cfg, prof, f"CREATE SCHEMA IF NOT EXISTS {fq}")
    run_sql(cfg, prof, f"GRANT EXTERNAL USE SCHEMA ON SCHEMA {fq} TO `{me}`")
    run_sql(cfg, prof, f"CREATE VOLUME IF NOT EXISTS {fq}.probe_logs")

    def make(name: str) -> str:
        run_sql(
            cfg,
            prof,
            f"CREATE TABLE IF NOT EXISTS {fq}.{name} USING ICEBERG AS SELECT 1 AS id",
        )
        return name

    with ThreadPoolExecutor(max_workers=8) as pool:
        for name in pool.map(make, cfg.table_names()):
            print(f"table ready: {name}")

    run_sql(
        cfg,
        prof,
        f"""CREATE TABLE IF NOT EXISTS {fq}.uniform_t000
        TBLPROPERTIES (
          'delta.enableIcebergCompatV2' = 'true',
          'delta.universalFormat.enabledFormats' = 'iceberg'
        ) AS SELECT 1 AS id""",
    )
    print("table ready: uniform_t000 (UniForm Delta, for the legacy /iceberg endpoint)")
    print(f"setup complete: {cfg.table_count} Iceberg tables + 1 UniForm table + volume")


if __name__ == "__main__":
    main()
