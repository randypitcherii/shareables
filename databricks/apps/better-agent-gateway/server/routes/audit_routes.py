from fastapi import APIRouter, HTTPException, status

from ..audit import get_audit_store

router = APIRouter()


@router.get("/v1/audit/requests/{request_id}")
def get_audit_request(request_id: str):
    record = get_audit_store().get(request_id)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Request audit not found",
        )
    return record
