# Research: Programmatic Cursor IDE Config Validation

## Best Approach: OpenAI Python SDK Against the Gateway Endpoint

Cursor uses standard OpenAI-compatible endpoints (`/v1/models`, `/v1/chat/completions`). The most practical validation is to **hit the gateway directly with the OpenAI Python SDK**, exactly as Cursor would internally. No Cursor-specific tooling needed.

```python
from openai import OpenAI

client = OpenAI(
    api_key="databricks-oauth",  # or the actual token
    base_url="https://my-gateway.aws.databricksapps.com/api/v1",
)

# Validation 1: List models (proves connectivity + auth)
models = client.models.list()
assert len(models.data) > 0, "No models returned"

# Validation 2: Test completion (proves end-to-end works)
resp = client.chat.completions.create(
    model="claude-haiku-latest",
    messages=[{"role": "user", "content": "Say hello in 3 words."}],
    max_tokens=30,
)
assert resp.choices[0].message.content, "Empty response"
```

**Why this is the best approach:**
- Tests the exact same HTTP contract Cursor uses
- No Cursor installation or UI automation needed
- Runnable in CI with `pip install openai`
- Already partially implemented in `scripts/validate.py` (using raw httpx)

## Fallback Approaches (Ranked)

### 1. Existing `scripts/validate.py` (already built)
The gateway already has a comprehensive validation script at `scripts/validate.py` that tests health, auth, model discovery, chat completion, scope enforcement, and identity attribution. It uses `httpx` directly rather than the OpenAI SDK. This could be adapted to also validate from the Cursor client perspective.

### 2. JSON Schema Validation of Config Output
Validate the generated config JSON matches Cursor's expected shape:
```python
CURSOR_CONFIG_SCHEMA = {
    "type": "object",
    "required": ["apiKey", "baseUrl", "models"],
    "properties": {
        "apiKey": {"type": "string", "minLength": 1},
        "baseUrl": {"type": "string", "format": "uri", "pattern": ".*/api/v1$"},
        "models": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1,
        },
    },
}
```

### 3. Playwright/Browser Automation (last resort)
Cursor is an Electron app, so Playwright cannot directly control it. You would need to use Cursor's web-based settings UI or a custom test harness. Not practical for CI.

## Key Findings

### Cursor CLI
- **Exists** at `/Users/randy.pitcher/.local/bin/cursor` (v2.6.19)
- Based on VS Code CLI, supports: `--diff`, `--merge`, `--goto`, `--new-window`, etc.
- **No config validation commands.** No `cursor validate`, `cursor test-connection`, or similar.
- Cannot list models or test API connections from CLI.

### Cursor Config File Locations
- **MCP config:** `~/.cursor/mcp.json` (global) or `.cursor/mcp.json` (project-level)
  - Schema: `{"mcpServers": {"name": {"command": "...", "args": [...], "env": {...}}}}`
- **OpenAI API key/base URL:** Stored in Cursor's internal state (likely SQLite/LevelDB under `~/.cursor/`), NOT in a user-editable JSON file. Configured via Settings > Models UI only.
- **CLI config:** `~/.cursor/cli-config.json` contains model preferences but not API keys/base URLs.

### Cursor API/SDK
- **No public API or SDK exists.** Cursor has no programmatic interface for managing settings.
- No npm/pip packages for Cursor config validation exist.
- The `rinadelph/CursorCustomModels` GitHub repo is a proxy server (similar concept to our gateway), not a config validation tool.

### OpenAI-Compatible Validation
- The OpenAI Python SDK (`openai` package) works with any compatible endpoint by setting `base_url`.
- Standard validation flow: `GET /v1/models` then `POST /v1/chat/completions`.
- Our gateway's `/v1/models` endpoint returns `{"aliases": {...}, "endpoints": [...]}` which is a custom format -- Cursor may expect the standard OpenAI format: `{"object": "list", "data": [{"id": "model-name", "object": "model", ...}]}`.

### Important Limitation: Cursor's OpenAI Base URL Override
- Cursor has a **single** "Override OpenAI Base URL" setting that applies to ALL models.
- Known bug: the toggle auto-enables itself if a URL value is present.
- Feature request exists for per-model base URLs (not yet implemented as of March 2026).
- This means configuring our gateway as the base URL will route ALL OpenAI model requests through it.

## Gaps -- What Cannot Be Validated Programmatically

1. **Cursor's internal config state** -- API key and base URL are stored internally, not in editable config files. Cannot verify Cursor "has" the right config without opening the UI.
2. **Cursor-specific request formatting** -- Cursor adds proprietary headers, tool-call schemas, and context. A raw OpenAI SDK test validates the endpoint works, but not that Cursor's specific request format is handled correctly.
3. **Auto-complete and Tab features** -- These use different model endpoints/protocols that may not go through the OpenAI-compatible path.
4. **The "Verify" button behavior** -- Cursor has a built-in "Verify" button for API keys that we cannot trigger programmatically.

## Recommended Next Steps

1. **Enhance `scripts/validate.py`** to also use the OpenAI Python SDK (not just raw httpx) so it tests the exact client contract.
2. **Ensure `/v1/models` returns OpenAI-standard format** -- Cursor expects `{"object": "list", "data": [{"id": "...", "object": "model"}]}`. The current response format (`aliases`/`endpoints`) may not be compatible.
3. **Add a pytest-based validation** that can run in CI against a deployed gateway.
4. **Document manual Cursor setup steps** since the API key + base URL must be configured through the UI.

## Relevant Links

- Cursor forum on base URL override: https://forum.cursor.com/t/custom-base-urls-for-each-custom-model/147219
- CursorCustomModels proxy: https://github.com/rinadelph/CursorCustomModels
- OpenAI Python SDK: https://github.com/openai/openai-python
- Existing validation script: `scripts/validate.py`
- Existing auth test: `scripts/headless_auth_test.py`
