from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any
import uuid


@dataclass(frozen=True)
class AuditRecord:
    request_id: str
    user_id: str
    action: str
    decision: str
    reason: str
    timestamp: str
    metadata: dict[str, Any]


class AuditStore:
    def __init__(self) -> None:
        self._records: dict[str, AuditRecord] = {}

    def add(
        self,
        *,
        user_id: str,
        action: str,
        decision: str,
        reason: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        request_id = str(uuid.uuid4())
        record = AuditRecord(
            request_id=request_id,
            user_id=user_id,
            action=action,
            decision=decision,
            reason=reason,
            timestamp=datetime.now(tz=timezone.utc).isoformat(),
            metadata=metadata or {},
        )
        self._records[request_id] = record
        return request_id

    def get(self, request_id: str) -> dict[str, Any] | None:
        record = self._records.get(request_id)
        if not record:
            return None
        return asdict(record)
