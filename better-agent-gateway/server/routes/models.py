from fastapi import APIRouter

router = APIRouter()


def _get_alias_registry():
    from ..alias_registry import get_registry
    return get_registry()


@router.get("/v1/models")
def list_models():
    registry = _get_alias_registry()
    return {
        "aliases": registry.list_aliases(),
        "endpoints": registry.list_endpoints(),
    }
