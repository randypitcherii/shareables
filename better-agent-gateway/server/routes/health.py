from fastapi import APIRouter

from server.version import version_dict

router = APIRouter()


@router.get("/v1/healthz")
def healthz():
    return {"status": "ok", **version_dict()}
