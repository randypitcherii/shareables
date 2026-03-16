from fastapi import APIRouter

from .health import router as health_router
from .metadata import router as metadata_router
from .proposals import router as proposals_router

api_router = APIRouter()
api_router.include_router(health_router, prefix="/api/v1", tags=["health"])
api_router.include_router(metadata_router, prefix="/api/v1/metadata", tags=["metadata"])
api_router.include_router(proposals_router, prefix="/api/v1/proposals", tags=["proposals"])
