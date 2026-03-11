from typing import Any
import uuid

from fastapi import APIRouter, Header, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from server.alias_registry import AliasRegistry
from server.audit import AuditStore
from server.policy import PolicyEngine

router = APIRouter()
alias_registry = AliasRegistry()
policy_engine = PolicyEngine()
audit_store = AuditStore()
sessions: dict[str, dict[str, Any]] = {}


class CreateSessionRequest(BaseModel):
    model_alias: str = Field(default="claude-sonnet-latest")


class SendMessageRequest(BaseModel):
    content: str
    model_alias: str | None = None


class ToolInvokeRequest(BaseModel):
    args: dict[str, Any] = Field(default_factory=dict)


def _require_forwarded_user_id(forwarded_user_id: str | None) -> str:
    if not forwarded_user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing forwarded user identity",
        )
    return forwarded_user_id


@router.post("/v1/chat/sessions", status_code=status.HTTP_201_CREATED)
def create_session(
    payload: CreateSessionRequest,
    x_forwarded_user_id: str | None = Header(default=None),
):
    user_id = _require_forwarded_user_id(x_forwarded_user_id)
    session_id = str(uuid.uuid4())
    resolved_model = alias_registry.resolve(payload.model_alias)
    sessions[session_id] = {"user_id": user_id, "resolved_model": resolved_model}
    request_id = audit_store.add(
        user_id=user_id,
        action="create_session",
        decision="allowed",
        reason="allowed",
        metadata={"session_id": session_id, "model_alias": payload.model_alias},
    )
    return {
        "session_id": session_id,
        "resolved_model": resolved_model,
        "request_id": request_id,
    }


@router.post("/v1/chat/sessions/{session_id}/messages")
def send_message(
    session_id: str,
    payload: SendMessageRequest,
    x_forwarded_user_id: str | None = Header(default=None),
):
    user_id = _require_forwarded_user_id(x_forwarded_user_id)
    session = sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
    if session["user_id"] != user_id:
        request_id = audit_store.add(
            user_id=user_id,
            action="send_message",
            decision="denied",
            reason="session_user_mismatch",
            metadata={"session_id": session_id},
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Session is not bound to current user",
            headers={"x-request-id": request_id},
        )

    model = alias_registry.resolve(payload.model_alias or session["resolved_model"])
    request_id = audit_store.add(
        user_id=user_id,
        action="send_message",
        decision="allowed",
        reason="allowed",
        metadata={"session_id": session_id, "model": model},
    )
    return {"request_id": request_id, "response": f"echo:{payload.content}", "model": model}


@router.post("/v1/tools/{tool:path}/invoke")
def invoke_tool(
    tool: str,
    payload: ToolInvokeRequest,
    x_forwarded_user_id: str | None = Header(default=None),
):
    user_id = _require_forwarded_user_id(x_forwarded_user_id)
    allowed, reason = policy_engine.authorize_tool(tool, payload.args)
    decision = "allowed" if allowed else "denied"
    request_id = audit_store.add(
        user_id=user_id,
        action=f"tool:{tool}",
        decision=decision,
        reason=reason,
        metadata={"args_keys": sorted(payload.args.keys())},
    )
    if not allowed:
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={
                "detail": "Tool invocation denied by policy",
                "request_id": request_id,
                "reason": reason,
            },
        )
    return {"request_id": request_id, "tool": tool, "status": "ok"}


@router.get("/v1/audit/requests/{request_id}")
def get_audit_request(request_id: str):
    record = audit_store.get(request_id)
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Request audit not found")
    return record


@router.get("/v1/healthz")
def healthz():
    return {"status": "ok"}


@router.get("/v1/policy/version")
def policy_version():
    return {"version": policy_engine.version}
