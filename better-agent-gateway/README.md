# llm-app-scoped-oauth

Databricks App API wrapper for user-scoped authorization and model alias routing.

## APIs

- `POST /v1/chat/sessions`
- `POST /v1/chat/sessions/{id}/messages`
- `POST /v1/tools/{tool}/invoke`
- `GET /v1/audit/requests/{request_id}`
- `GET /v1/healthz`
- `GET /v1/policy/version`

## Local test

```bash
uv sync
uv run pytest tests/test_api_contract.py -q
```
