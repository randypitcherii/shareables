import asyncio

from fastapi import APIRouter, Depends, HTTPException
from starlette.concurrency import run_in_threadpool

from ..auth import RequestContext, get_request_context
from ..config import settings
from ..db import ensure_schema, get_pool, save_proposal
from ..models import (
    ConfirmProposalRequest,
    ConfirmProposalResponse,
    GenerateProposalRequest,
    GenerateProposalResponse,
)
from ..services.proposal_generator import generate_changes
from ..services.uc_metadata import get_object_detail

router = APIRouter()


@router.post("/generate", response_model=GenerateProposalResponse)
async def generate_proposals(
    request: GenerateProposalRequest,
    context: RequestContext = Depends(get_request_context),
) -> GenerateProposalResponse:
    try:
        detail = await asyncio.wait_for(
            run_in_threadpool(
                get_object_detail,
                access_token=context.access_token,
                catalog_name=request.catalog_name,
                schema_name=request.schema_name,
                object_name=request.object_name,
            ),
            timeout=settings.metadata_query_timeout_seconds,
        )
        source, changes = await asyncio.wait_for(
            run_in_threadpool(generate_changes, detail, context.access_token),
            timeout=settings.metadata_query_timeout_seconds,
        )
        return GenerateProposalResponse(source=source, changes=changes)
    except TimeoutError as exc:
        raise HTTPException(
            status_code=504,
            detail=(
                f"Timed out generating proposals after "
                f"{settings.metadata_query_timeout_seconds}s. "
                "Verify SQL warehouse/model endpoint availability and permissions."
            ),
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/confirm", response_model=ConfirmProposalResponse)
async def confirm_proposals(
    request: ConfirmProposalRequest,
    context: RequestContext = Depends(get_request_context),
) -> ConfirmProposalResponse:
    try:
        pool = await get_pool(context.access_token)
        if not pool:
            return ConfirmProposalResponse(
                proposal_id=None,
                stored=False,
                storage_mode="lakebase-not-configured",
                message="Lakebase env vars are not configured. Proposal was reviewed but not persisted.",
            )

        await ensure_schema(pool)
        proposal_id = await save_proposal(
            pool,
            created_by=context.user_name,
            catalog_name=request.catalog_name,
            schema_name=request.schema_name,
            object_name=request.object_name,
            object_type=request.object_type,
            reviewer_notes=request.reviewer_notes,
            changes=[change.model_dump() for change in request.changes],
        )
        return ConfirmProposalResponse(
            proposal_id=proposal_id,
            stored=True,
            storage_mode="lakebase-postgres",
            message="Proposal saved to Lakebase metadata_review.proposals.",
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
