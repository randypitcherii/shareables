from fastapi import APIRouter

router = APIRouter()


@router.get("/v1/healthz")
def healthz():
    return {"status": "ok"}
