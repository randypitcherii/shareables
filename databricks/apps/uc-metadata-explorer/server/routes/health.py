from datetime import datetime, timezone

from fastapi import APIRouter

from ..config import settings
from ..db import lakebase_configured

router = APIRouter()


@router.get("/healthcheck")
def healthcheck() -> dict:
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "mode": "databricks-app" if settings.is_databricks_app else "local",
        "lakebase_configured": lakebase_configured(),
        "warehouse_configured": bool(settings.sql_warehouse_id),
    }
