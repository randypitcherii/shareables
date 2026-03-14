from fastapi import APIRouter

from .audit_routes import router as audit_router
from .auth import router as auth_router
from .chat import router as chat_router
from .health import router as health_router
from .logs import router as logs_router
from .models import router as models_router
from .permissions import router as permissions_router
from .shell import router as shell_router
from .version import router as version_router

api_router = APIRouter(prefix="/api")
api_router.include_router(audit_router, tags=["audit"])
api_router.include_router(auth_router, tags=["auth"])
api_router.include_router(chat_router, tags=["chat"])
api_router.include_router(health_router, tags=["health"])
api_router.include_router(logs_router, tags=["logs"])
api_router.include_router(models_router, tags=["models"])
api_router.include_router(permissions_router, tags=["permissions"])
api_router.include_router(shell_router, tags=["shell"])
api_router.include_router(version_router, tags=["version"])
