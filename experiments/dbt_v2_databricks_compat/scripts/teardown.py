"""Drop every schema the experiment created and destroy the bundle's jobs."""

from __future__ import annotations

import subprocess

from _common import CATALOG, EXPERIMENT_ROOT, SCHEMA, client, sql
from run_jobs import bundle_vars


def main() -> None:
    w = client()
    for suffix in ("", "_native", "_runner", "_dbt_test__audit"):
        sql(w, f"DROP SCHEMA IF EXISTS `{CATALOG}`.`{SCHEMA}{suffix}` CASCADE")
        print(f"dropped {CATALOG}.{SCHEMA}{suffix}")
    subprocess.run(
        ["databricks", "bundle", "destroy", "--auto-approve", *bundle_vars(w)],
        cwd=EXPERIMENT_ROOT,
        check=True,
    )


if __name__ == "__main__":
    main()
