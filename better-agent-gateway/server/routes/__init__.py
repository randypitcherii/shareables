from fastapi import APIRouter

from .chat import router as chat_router
from .models import router as models_router
from .health import router as health_router
from .audit_routes import router as audit_router

api_router = APIRouter(prefix="/api")
api_router.include_router(chat_router, tags=["chat"])
api_router.include_router(models_router, tags=["models"])
api_router.include_router(health_router, tags=["health"])
api_router.include_router(audit_router, tags=["audit"])
