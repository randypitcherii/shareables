"""Remove everything the probe created in the workspace. Idempotent.

Drops the dashboard (if data/state.json has one) and the probe schema with its tables,
views, and volume. The sandbox workspaces themselves are transient and expire on
their own; delete them early from the sandbox portal if you are done.
"""

import json

from _common import DATA_DIR, load_config, run_sql, workspace_client


def main() -> None:
    cfg = load_config()
    state_file = DATA_DIR / "state.json"
    state = json.loads(state_file.read_text()) if state_file.exists() else {}
    if state.get("dashboard_id"):
        try:
            workspace_client(cfg.profile("a")).lakeview.trash(state["dashboard_id"])
            print("dashboard trashed")
        except Exception as e:  # already gone
            print(f"dashboard: {e}")
        state.pop("dashboard_id")
        state_file.write_text(json.dumps(state, indent=2))
    for label in cfg.labels():
        run_sql(cfg, label, f"DROP SCHEMA IF EXISTS `{cfg.catalog(label)}`.`{cfg.uc_schema}` CASCADE")
        print(f"workspace {label}: schema {cfg.uc_schema} dropped (tables, views, volume)")


if __name__ == "__main__":
    main()
