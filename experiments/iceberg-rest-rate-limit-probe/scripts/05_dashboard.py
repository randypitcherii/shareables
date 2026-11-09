"""Deploy (or update) and publish the AI/BI dashboard over the probe views. Idempotent.

The dashboard ID is kept in data/state.json so cleanup can remove it.
"""

import json

from _common import DATA_DIR, EXPERIMENT_ROOT, load_config, workspace_client
from databricks.sdk.service.dashboards import Dashboard

NAME = "Iceberg REST rate-limit probe"
STATE = DATA_DIR / "state.json"


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg.profile("a"))
    fq = f"`{cfg.catalog('a')}`.`{cfg.uc_schema}`"
    body = (EXPERIMENT_ROOT / "dashboard" / "iceberg_rest_probe.lvdash.json").read_text()
    body = body.replace("__FQ__", fq.replace("`", "\\u0060"))
    me = w.current_user.me().user_name
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    dash = Dashboard(display_name=NAME, serialized_dashboard=body, warehouse_id=cfg.warehouse("a"),
                     parent_path=f"/Users/{me}")
    if state.get("dashboard_id"):
        d = w.lakeview.update(state["dashboard_id"], dashboard=dash)
    else:
        d = w.lakeview.create(dashboard=dash)
    w.lakeview.publish(d.dashboard_id, warehouse_id=cfg.warehouse("a"), embed_credentials=True)
    state["dashboard_id"] = d.dashboard_id
    DATA_DIR.mkdir(exist_ok=True)
    STATE.write_text(json.dumps(state, indent=2))
    print(f"dashboard published: {w.config.host}/dashboardsv3/{d.dashboard_id}/published")


if __name__ == "__main__":
    main()
