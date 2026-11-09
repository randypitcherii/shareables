"""Load raw probe logs into a Delta table and (re)create the dashboard views.

Uploads data/<run_id>/*.parquet to the probe_logs volume, rebuilds `probe_calls` from
every file in the volume, then applies sql/views.sql. Idempotent.
"""

import argparse

from _common import DATA_DIR, EXPERIMENT_ROOT, load_config, run_sql, workspace_client


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--runs", default="", help="comma list of run ids (default: all under data/)")
    p.add_argument("--views-only", action="store_true")
    args = p.parse_args()
    cfg = load_config()
    prof = "a"
    fq = f"`{cfg.catalog('a')}`.`{cfg.uc_schema}`"
    vol = f"/Volumes/{cfg.catalog('a')}/{cfg.uc_schema}/probe_logs"

    if not args.views_only:
        w = workspace_client(cfg.profile(prof))
        dirs = ([DATA_DIR / r for r in args.runs.split(",")] if args.runs
                else sorted(d for d in DATA_DIR.iterdir() if d.is_dir()))
        n = 0
        for d in dirs:
            for f in sorted(d.glob("*.parquet")):
                with open(f, "rb") as fh:
                    w.files.upload(f"{vol}/runs/{d.name}/{f.name}", fh, overwrite=True)
                n += 1
        print(f"uploaded {n} parquet files to {vol}")
        # run_id := "<run folder>/<file stem>" so solo and scope runs of one endpoint stay apart
        run_sql(cfg, prof, f"""CREATE OR REPLACE TABLE {fq}.probe_calls AS
            SELECT * EXCEPT (run_id),
                   regexp_extract(_metadata.file_path, '/runs/(.+)[.]parquet$', 1) AS run_id
            FROM read_files('{vol}/runs/', format => 'parquet', recursiveFileLookup => true,
                            pathGlobFilter => '*.parquet')""")
        rows = run_sql(cfg, prof, f"SELECT COUNT(*) FROM {fq}.probe_calls")
        print(f"probe_calls rows: {rows[0][0]}")

    sql = (EXPERIMENT_ROOT / "sql" / "views.sql").read_text().replace("{fq}", fq)
    for stmt in _statements(sql):
        run_sql(cfg, prof, stmt)
        print("ok:", stmt.split("\n", 1)[0][:90])


def _statements(sql: str) -> list[str]:
    body = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))
    return [s.strip() for s in body.split(";") if s.strip()]


if __name__ == "__main__":
    main()
