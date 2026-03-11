# Better Agent Gateway

A Databricks App that proxies AI Gateway serving endpoints with:

- **Scoped OAuth** — users authenticate as themselves (SSO), but the app limits their token scope to only LLM serving endpoint access
- **Automatic `-latest` aliases** — `claude-sonnet-latest`, `gpt-4o-latest`, `gemini-latest`, etc. auto-resolve to the newest deployed model version

## Architecture

```
User -> Databricks App (OAuth scoped) -> Better Agent Gateway -> Serving Endpoints
                                              |
                                    OBO token forwarded
                                    (user's identity, limited scope)
```

The key security mechanism: the proxy layer only forwards requests to `/serving-endpoints/*/invocations`. Even if a user is a workspace admin, their token through this app can only perform LLM inference.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/chat/completions` | Proxy chat completion to resolved serving endpoint |
| GET | `/api/v1/models` | List available aliases and endpoints |
| GET | `/api/v1/healthz` | Health check |
| GET | `/api/v1/audit/requests/{id}` | Audit trail lookup |

## Model Families

| Alias | Example Resolution |
|-------|-------------------|
| `claude-sonnet-latest` | `databricks-claude-sonnet-4-6` |
| `claude-opus-latest` | `databricks-claude-opus-4-6` |
| `claude-haiku-latest` | `databricks-claude-haiku-4-5` |
| `gpt-4o-latest` | `databricks-gpt-4o` |
| `gpt-4o-mini-latest` | `databricks-gpt-4o-mini` |
| `gemini-latest` | `databricks-gemini-2-0-flash` |
| `codex-latest` | `databricks-codex-mini-latest` |

## Local Development

```bash
# Backend
cd better-agent-gateway
uv sync
APP_ENV=dev uv run uvicorn app:app --reload --port 8000

# Frontend
cd frontend
npm install
npm run dev
```

## Deploy with DABs

```bash
cd better-agent-gateway
databricks bundle deploy --target dev
```

## Run Tests

```bash
uv run pytest tests/ -v
cd frontend && npm test
```
