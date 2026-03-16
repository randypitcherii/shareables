import asyncio

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from starlette.concurrency import run_in_threadpool

from ..auth import RequestContext, get_request_context
from ..config import settings
from ..models import MetadataObjectDetail, MetadataObjectSummary
from ..services.local_cache import get_object_detail_cached, list_objects_cached
from ..services.uc_metadata import (
    MetadataPrimaryUnavailableError,
    get_object_detail,
    list_objects_fallback,
    list_objects_primary,
)

router = APIRouter()


@router.get("/objects", response_model=list[MetadataObjectSummary])
async def get_objects(
    response: Response,
    search: str = Query(default=""),
    gap_only: bool = Query(default=True),
    limit: int = Query(default=settings.limit_default, ge=1, le=settings.limit_max),
    fallback: bool = Query(
        default=False,
        description="When true, bypass the primary warehouse query and use fallback listing.",
    ),
    source: str = Query(
        default="auto",
        description="Data source mode: auto, primary, fallback, or cache.",
    ),
    context: RequestContext = Depends(get_request_context),
) -> list[MetadataObjectSummary]:
    if source == "fallback":
        fallback = True

    if source == "cache":
        response.headers["x-uc-metadata-mode"] = "cache"
        return list_objects_cached(search=search, gap_only=gap_only, limit=limit)

    if fallback:
        response.headers["x-uc-metadata-mode"] = "fallback"
        response.headers["x-uc-metadata-fallback-reason"] = "user_requested"
        return list_objects_fallback(search=search, gap_only=gap_only, limit=limit)

    try:
        result = await asyncio.wait_for(
            run_in_threadpool(
                list_objects_primary,
                access_token=context.access_token,
                search=search,
                gap_only=gap_only,
                limit=limit,
            ),
            timeout=settings.metadata_query_timeout_seconds,
        )
        response.headers["x-uc-metadata-mode"] = "primary"
        return result
    except TimeoutError as exc:
        raise HTTPException(
            status_code=503,
            detail=(
                f"Primary metadata query timed out after {settings.metadata_query_timeout_seconds}s. "
                "The SQL warehouse may be cold, overloaded, or unreachable."
            ),
            headers={
                "x-uc-metadata-primary-failed": "true",
                "x-uc-metadata-primary-reason": "timeout",
                "x-uc-metadata-fallback-available": "true",
            },
        ) from exc
    except MetadataPrimaryUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail=str(exc),
            headers={
                "x-uc-metadata-primary-failed": "true",
                "x-uc-metadata-primary-reason": exc.reason_code,
                "x-uc-metadata-fallback-available": "true",
            },
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Primary metadata query failed unexpectedly.",
            headers={
                "x-uc-metadata-primary-failed": "true",
                "x-uc-metadata-primary-reason": "query_error",
                "x-uc-metadata-fallback-available": "true",
            },
        ) from exc


@router.get("/objects/{catalog_name}/{schema_name}/{object_name}", response_model=MetadataObjectDetail)
async def get_object(
    catalog_name: str,
    schema_name: str,
    object_name: str,
    source: str = Query(default="auto", description="Data source mode: auto or cache."),
    context: RequestContext = Depends(get_request_context),
) -> MetadataObjectDetail:
    if source == "cache":
        try:
            return get_object_detail_cached(
                catalog_name=catalog_name,
                schema_name=schema_name,
                object_name=object_name,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    try:
        return await asyncio.wait_for(
            run_in_threadpool(
                get_object_detail,
                access_token=context.access_token,
                catalog_name=catalog_name,
                schema_name=schema_name,
                object_name=object_name,
            ),
            timeout=settings.metadata_query_timeout_seconds,
        )
    except TimeoutError as exc:
        raise HTTPException(
            status_code=504,
            detail=(
                f"Timed out querying object details after "
                f"{settings.metadata_query_timeout_seconds}s. "
                "Verify SQL warehouse availability and permissions."
            ),
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
