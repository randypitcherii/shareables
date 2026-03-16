from __future__ import annotations

import json
from typing import Any

import requests

from ..config import get_workspace_host, settings
from ..models import MetadataObjectDetail, ProposedChange


def _friendly_label(name: str) -> str:
    return name.replace("_", " ").replace("-", " ").strip()


def _infer_column_description(column_name: str, data_type: str) -> str:
    normalized = column_name.lower()
    label = _friendly_label(column_name)
    if normalized.endswith("_id") or normalized == "id":
        return f"Unique identifier for the {label.replace(' id', '')}."
    if normalized.endswith("_ts") or "timestamp" in normalized:
        return f"Timestamp when {label} was recorded."
    if normalized.endswith("_date") or normalized.endswith("_dt"):
        return f"Date associated with {label}."
    if "amount" in normalized or "price" in normalized or "cost" in normalized:
        return f"Monetary value for {label} in the source system."
    if data_type.lower().startswith(("int", "bigint", "double", "decimal", "float")):
        return f"Numeric value representing {label}."
    return f"Business meaning for {label}."


def _heuristic_changes(detail: MetadataObjectDetail) -> list[ProposedChange]:
    changes: list[ProposedChange] = []
    summary = detail.summary

    if summary.table_description_missing:
        changes.append(
            ProposedChange(
                target_type="table",
                target_name=summary.object_name,
                field="description",
                current_value=summary.comment,
                proposed_value=(
                    f"Curated {summary.object_type.lower()} in {summary.schema_name} "
                    f"containing {_friendly_label(summary.object_name)} records."
                ),
                rationale="Fill missing table description so users can understand table purpose quickly.",
            )
        )

    if summary.table_tags_missing:
        changes.append(
            ProposedChange(
                target_type="table",
                target_name=summary.object_name,
                field="tags",
                current_value=None,
                proposed_value=json.dumps(
                    [
                        {"name": "domain", "value": summary.schema_name},
                        {"name": "quality_reviewed", "value": "false"},
                    ]
                ),
                rationale="Seed table tags for domain discoverability and review workflow tracking.",
            )
        )

    if summary.table_owner_missing:
        changes.append(
            ProposedChange(
                target_type="table",
                target_name=summary.object_name,
                field="owner",
                current_value=summary.owner,
                proposed_value=f"{summary.schema_name}_data_stewards",
                rationale="Suggest a steward group owner when owner metadata is missing.",
            )
        )

    for column in detail.columns:
        if column.description_missing:
            changes.append(
                ProposedChange(
                    target_type="column",
                    target_name=column.column_name,
                    field="description",
                    current_value=column.comment,
                    proposed_value=_infer_column_description(column.column_name, column.data_type),
                    rationale="Column lacks description and needs basic semantic documentation.",
                )
            )
        if column.tags_missing:
            changes.append(
                ProposedChange(
                    target_type="column",
                    target_name=column.column_name,
                    field="tags",
                    current_value=None,
                    proposed_value=json.dumps([{"name": "classification", "value": "to-review"}]),
                    rationale="Mark untagged column for governance review and downstream policy setup.",
                )
            )

    return changes


def _llm_changes(detail: MetadataObjectDetail, access_token: str) -> list[ProposedChange] | None:
    if not settings.serving_endpoint or not access_token:
        return None
    host = get_workspace_host().rstrip("/")
    if not host:
        return None

    prompt_payload = {
        "table": detail.summary.model_dump(),
        "table_tags": [tag.model_dump() for tag in detail.table_tags],
        "columns": [column.model_dump() for column in detail.columns],
    }
    request_payload = {
        "messages": [
            {
                "role": "system",
                "content": (
                    "You propose metadata updates for Unity Catalog. "
                    "Return strict JSON with shape: {\"changes\": [...]} where each change has "
                    "target_type, target_name, field, current_value, proposed_value, rationale. "
                    "Only propose missing descriptions, missing tags, or missing owner."
                ),
            },
            {
                "role": "user",
                "content": f"Create metadata proposals for this object:\n{json.dumps(prompt_payload)}",
            },
        ],
        "max_tokens": 1200,
        "temperature": 0.2,
    }
    response = requests.post(
        f"{host}/serving-endpoints/{settings.serving_endpoint}/invocations",
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        json=request_payload,
        timeout=30,
    )
    if response.status_code >= 400:
        return None

    data = response.json()
    content = (
        data.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
    )
    if not content:
        return None

    start = content.find("{")
    end = content.rfind("}")
    if start == -1 or end == -1:
        return None
    candidate = json.loads(content[start : end + 1])
    raw_changes = candidate.get("changes", [])
    parsed: list[ProposedChange] = []
    for item in raw_changes:
        parsed.append(
            ProposedChange(
                target_type=item.get("target_type", "column"),
                target_name=item.get("target_name", ""),
                field=item.get("field", "description"),
                current_value=item.get("current_value"),
                proposed_value=item.get("proposed_value", ""),
                rationale=item.get("rationale", "AI-suggested metadata completion."),
            )
        )
    return parsed


def generate_changes(detail: MetadataObjectDetail, access_token: str) -> tuple[str, list[ProposedChange]]:
    try:
        llm_result = _llm_changes(detail, access_token)
        if llm_result:
            return ("foundation-model", llm_result)
    except Exception:
        pass
    return ("heuristic-fallback", _heuristic_changes(detail))
