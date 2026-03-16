# Research: Making Cursor IDE Work with Databricks App OAuth Gateway

**Date:** 2026-03-13
**Branch:** `feat/cursor-config-endpoint`
**Status:** Research complete, ready for implementation decision

---

## Executive Summary

Cursor IDE sends the `apiKey` field as a `Bearer` token in the `Authorization` header on every request. It has no built-in OAuth flow for custom OpenAI-compatible endpoints, no token refresh mechanism for model providers, and no plugin/extension system for custom auth. The `apiKey` must be a static, valid token.

**Recommended approach: Gateway-Issued API Keys (Approach 4)** — the gateway generates per-user long-lived API keys via browser SSO, and users paste them into Cursor. This is the only approach that delivers the "single copyable config" requirement without requiring users to install additional software.

---

## How Cursor Handles Auth for Custom Model Providers

### Key Findings

1. **Header behavior:** Cursor sends `Authorization: Bearer <apiKey>` on every request to the configured base URL. There is no way to customize the header name or format.

2. **Single base URL limitation:** Cursor has ONE "Override OpenAI Base URL" that applies to ALL OpenAI-model requests. A known bug: the toggle auto-enables when a URL is present. Per-model base URLs are a requested but unimplemented feature (as of March 2026).

3. **No OAuth for model providers:** Cursor's OAuth support is limited to MCP servers (and even there, refresh tokens are buggy — Cursor stores but doesn't use them, per multiple confirmed bug reports on forum.cursor.com). There is zero OAuth support for chat completion endpoints.

4. **No token refresh:** Tokens are static. If a Bearer token expires, requests fail with 401. Cursor does NOT attempt to refresh or re-auth for model provider endpoints.

5. **No credential helpers:** Unlike git (which supports credential helpers), Cursor has no mechanism to call an external process to obtain a fresh token.

6. **API keys in secure storage:** Cursor stores API keys in OS secure storage (Keychain on macOS), not in regular config files or SQLite DBs. They cannot be programmatically updated via scripts.

7. **Cursor Agent mode does not support custom API keys at all** (per LiteLLM docs). Only Ask and Plan modes work with BYOK.

### What This Means

Any solution MUST provide a token that:
- Is static (doesn't expire during a work session, ideally days/weeks)
- Can be pasted into Cursor's "OpenAI API Key" field
- Will be sent as `Authorization: Bearer <token>` and accepted by our gateway

---

## Approaches Evaluated

### Approach 1: Local Proxy Daemon ❌ Does Not Meet Requirements

**Concept:** User runs a local proxy (e.g., `localhost:8080`) that intercepts Cursor requests, injects a fresh OAuth token, and forwards to the real gateway.

**How it works:**
1. User installs and runs a Python/Node script locally
2. Cursor points to `http://localhost:8080/v1` as base URL
3. Proxy runs `databricks auth token` or holds a refresh token to get fresh OAuth tokens
4. Proxy forwards requests to the real gateway with the fresh token

**Pros:**
- Automatic token refresh (fully transparent)
- No changes to gateway needed
- Prior art: LiteLLM, cursor-api-proxy, LLM-API-Key-Proxy all use this pattern

**Cons:**
- **Violates "single copyable config" requirement** — users must install software
- Cursor doesn't recognize `localhost` addresses (confirmed in forums) — requires ngrok or similar tunneling, which adds more complexity
- Different setup per OS (macOS vs Windows vs Linux)
- Users must keep the proxy running
- Debugging auth failures becomes harder (is it Cursor? the proxy? the gateway?)

**Effort:** Medium (2-3 days to build, ongoing support burden)
**Verdict:** Rejected — the install/run requirement makes this a non-starter for the target UX.

---

### Approach 2: Browser-Based Token Copy (Short-Lived) ❌ Doesn't Scale

**Concept:** User visits gateway URL → SSO → gateway shows a page with a "Copy Token" button. Token is a raw Databricks OAuth access token.

**How it works:**
1. User visits `https://gateway.databricksapps.com/`
2. Databricks Apps proxy handles SSO redirect
3. Gateway UI shows: "Your Cursor API Key: `eyJ...`" with copy button
4. User pastes into Cursor

**Pros:**
- Zero software install
- True SSO via Databricks Apps proxy
- Simple implementation

**Cons:**
- **OAuth tokens expire in 1 hour** — user must re-copy hourly
- No way to extend Databricks OAuth token lifetime (hard-coded at the platform level)
- Terrible UX for daily use

**Effort:** Low (0.5 day)
**Verdict:** Rejected — hourly re-auth is not acceptable.

---

### Approach 3: OAuth Device Flow ❌ Not Supported

**Concept:** Implement RFC 8628 device authorization grant. User runs a command, gets a code, enters it in browser, gateway exchanges for a token.

**How it works:**
1. Gateway implements `/device/code` and `/device/token` endpoints
2. User (or a CLI tool) requests a device code
3. User enters code at gateway URL in browser
4. Gateway polls for completion, issues a token
5. User pastes token into Cursor

**Pros:**
- Industry standard (GitHub CLI, Docker, AWS SSO use this)
- Clean UX for CLI tools

**Cons:**
- **Databricks does not support device flow** — there's no device authorization endpoint in the Databricks OAuth spec
- Would require the gateway to act as its own authorization server (massive complexity)
- Still results in a short-lived token unless we also implement Approach 4

**Effort:** High (1-2 weeks, essentially building an OAuth AS)
**Verdict:** Rejected — Databricks platform doesn't support it, and it's overkill.

---

### Approach 4: Gateway-Issued API Keys ✅ RECOMMENDED

**Concept:** The gateway generates per-user, long-lived API keys via a browser-based flow. User visits the gateway, authenticates via SSO, and gets an API key they paste into Cursor. The gateway maps the API key back to the user's identity on each request.

**How it works:**

1. **User visits** `https://gateway.databricksapps.com/`
2. **SSO happens automatically** — Databricks Apps proxy redirects to IdP, sets session cookie
3. **Gateway UI** shows a "Generate Cursor API Key" button
4. **On click**, the gateway:
   - Extracts user identity from `x-forwarded-access-token` / `x-forwarded-email` headers
   - Generates a cryptographically random API key (e.g., `bag-xxxxxxxxxxxx`)
   - Stores a mapping: `api_key → {user_id, email, created_at, expires_at}`
   - Displays the key with a "Copy to Clipboard" button and the full Cursor config JSON
5. **User copies** the config JSON and pastes it into Cursor settings
6. **On each request** from Cursor:
   - Cursor sends `Authorization: Bearer bag-xxxxxxxxxxxx`
   - Gateway auth middleware looks up the API key in its store
   - If valid, the gateway uses the **app's own service principal** credentials to call downstream Databricks APIs
   - Audit log records: which user (from the key mapping), which model, when

**Architecture detail — identity vs. access:**
- The API key identifies the USER (for audit, rate limiting, permissions)
- The GATEWAY'S service principal provides the actual OAuth credentials to call Databricks serving endpoints
- This is a deliberate shift: instead of OBO (on-behalf-of-user), the gateway acts on behalf of itself but tracks who asked

**Pros:**
- **Single copyable config** — user copies one JSON blob, done
- **No software to install** — browser-only setup
- **Long-lived keys** — can be 30, 90, 180 days (configurable)
- **SSO for issuance** — key generation requires SSO, not the key usage
- **Revocable** — admin or user can revoke keys via the gateway UI
- **Auditable** — every request is logged with user identity from key mapping
- **Works with Cursor's model** — static Bearer token, exactly what Cursor expects

**Cons:**
- **Loses per-user OAuth scoping** — requests use the app's service principal, not the user's OBO token. The gateway must enforce permissions in its own policy layer.
- **Key storage** — gateway needs persistent storage (could be a Databricks SQL table, SQLite, or workspace-level secret scope)
- **Key rotation** — need a mechanism for users to rotate keys (UI button)
- **Security surface** — a leaked API key grants access until revoked (mitigated by: expiry, IP allowlisting, rate limiting, audit logging)

**Effort:** Medium (3-5 days)
**Verdict:** RECOMMENDED — best balance of UX and security.

---

### Approach 5: `databricks auth token` with Manual Paste ❌ Poor UX

**Concept:** User runs `databricks auth token --host <workspace>` in terminal, copies the output token, pastes into Cursor.

**How it works:**
1. User has Databricks CLI configured with `databricks auth login`
2. User runs `databricks auth token` to get a fresh token
3. Pastes into Cursor's API Key field

**Pros:**
- No gateway changes needed
- Uses standard Databricks OAuth

**Cons:**
- **Token expires in 1 hour** — same problem as Approach 2
- Requires Databricks CLI installed
- Manual terminal → Cursor copy-paste loop
- Not "single copyable config" — requires repeated action

**Effort:** Zero (already possible today)
**Verdict:** Rejected — too manual, same expiry problem.

---

### Approach 6: Hybrid — Gateway API Keys + OBO Token Caching 🔄 RUNNER-UP

**Concept:** Like Approach 4, but the gateway caches the user's OBO token from the key-generation session and attempts to use it (with refresh) for downstream calls. Falls back to the app's service principal when the OBO token expires.

**How it works:**
1. Same browser-based key generation as Approach 4
2. During key generation, gateway also stores the user's OBO access token and (if available) refresh token
3. On API requests, gateway tries the cached OBO token first
4. If expired and no refresh token available, falls back to app service principal

**Pros:**
- Preserves per-user identity for downstream calls (while OBO token is fresh)
- Same UX as Approach 4

**Cons:**
- **Databricks OBO tokens cannot be refreshed by the app** — the Databricks Apps proxy handles the OAuth flow, and the app only gets an access token (no refresh token is exposed to the app)
- Adds complexity with minimal benefit (OBO token lasts ~1 hour, then falls back anyway)
- Storing user OAuth tokens server-side is a security concern

**Effort:** High (5-7 days)
**Verdict:** Not worth the added complexity over Approach 4.

---

## Recommended Implementation: Gateway-Issued API Keys

### Implementation Sketch

#### 1. Storage Layer
```python
# Could use Databricks SQL, SQLite, or in-memory dict (for MVP)
# Schema:
{
    "api_key_hash": "sha256_of_key",  # Never store plaintext
    "user_email": "user@company.com",
    "user_id": "12345",
    "display_name": "Jane Doe",
    "created_at": "2026-03-13T10:00:00Z",
    "expires_at": "2026-06-13T10:00:00Z",  # 90-day default
    "last_used_at": null,
    "revoked": false
}
```

#### 2. Key Generation Endpoint
```
POST /api/v1/keys/generate
# Requires: browser session (x-forwarded-access-token present)
# Returns: { "api_key": "bag-xxxxx", "expires_at": "...", "cursor_config": {...} }
```

#### 3. Auth Middleware Update
```python
# In server/auth.py, update extract_request_context:
# 1. Check x-forwarded-access-token (existing OBO path)
# 2. Check Authorization: Bearer for "bag-" prefix → look up in key store
# 3. Check Authorization: Bearer for raw Databricks token (existing path)
```

#### 4. Gateway UI Page
- "Generate API Key" button (only visible when authenticated via browser)
- Shows generated key ONCE (never shown again)
- Shows copyable Cursor config JSON:
  ```json
  {
    "apiKey": "bag-xxxxxxxxxxxx",
    "baseUrl": "https://gateway.databricksapps.com/api/v1",
    "models": ["claude-sonnet-latest", "claude-haiku-latest"]
  }
  ```
- "Revoke Key" button for existing keys
- Table of active keys with last-used timestamps

### User Experience (Step by Step)

1. Open browser, navigate to `https://gateway.databricksapps.com/`
2. SSO redirect happens automatically (company IdP)
3. Land on gateway dashboard
4. Click "Generate Cursor API Key"
5. Copy the displayed JSON config
6. Open Cursor → Settings → Models
7. Paste API key into "OpenAI API Key" field
8. Enable "Override OpenAI Base URL" → paste the base URL
9. Add custom models (claude-sonnet-latest, etc.)
10. Done. Works for 90 days (or until revoked).

### Security Analysis

| Risk | Mitigation |
|------|-----------|
| Leaked API key | Keys have configurable TTL (default 90 days). Keys are revocable via UI. Audit log shows all usage. |
| Key stored server-side | Only SHA-256 hash stored. Plaintext shown once at generation. |
| No per-user OAuth scoping | Gateway's policy layer enforces model access. App service principal is scoped to `serving-endpoints` only. |
| Replay attacks | Rate limiting per key. Optional: IP allowlist per key. |
| Key enumeration | Keys use 256-bit random values with `bag-` prefix. Infeasible to guess. |
| Admin visibility | Gateway admin can see all keys, their owners, and usage. Can revoke any key. |

### Trade-off: OBO vs. Service Principal

The biggest architectural decision is that API-key-authenticated requests will use the **gateway's service principal** to call downstream Databricks serving endpoints, NOT the user's own OAuth token. This means:

- **Lost:** Per-user permissions at the Databricks API level (Unity Catalog, endpoint ACLs)
- **Gained:** Long-lived auth that works with Cursor's static-key model
- **Mitigation:** The gateway enforces its own permission layer (`server/policy.py`) and audit logging (`server/audit.py`)

If per-user scoping is critical, the OBO path still works for browser-based access and `databricks auth token` users — API keys are an additional auth path, not a replacement.

---

## Effort Estimates

| Approach | Effort | Meets All Requirements |
|----------|--------|----------------------|
| 1. Local Proxy | 2-3 days | No (requires install) |
| 2. Browser Token Copy | 0.5 day | No (hourly expiry) |
| 3. Device Flow | 1-2 weeks | No (not supported by Databricks) |
| **4. Gateway API Keys** | **3-5 days** | **Yes** |
| 5. Manual CLI Token | 0 days | No (hourly expiry) |
| 6. Hybrid OBO Cache | 5-7 days | Partially (falls back to SP anyway) |

---

## Relevant Links

- **Cursor auth behavior:** Sends `Authorization: Bearer <apiKey>` — confirmed via [forum posts](https://forum.cursor.com/t/set-custom-open-ai-api-key/58974) and LiteLLM integration docs
- **Cursor OAuth bug (MCP):** Refresh tokens stored but not used — [forum.cursor.com/t/refresh-tokens-are-not-used/141276](https://forum.cursor.com/t/refresh-tokens-are-not-used/141276)
- **Cursor base URL limitation:** Single override, affects all models — [forum.cursor.com/t/cursor-models-fail-when-using-byok/147218](https://forum.cursor.com/t/cursor-models-fail-when-using-byok-openai-key-with-overridden-base-url-glm-4-7/147218)
- **LiteLLM Cursor integration:** Documents the base URL + API key pattern — [docs.litellm.ai/docs/tutorials/cursor_integration](https://docs.litellm.ai/docs/tutorials/cursor_integration)
- **Databricks Apps auth:** OBO token flow, scopes — [docs.databricks.com/aws/en/dev-tools/databricks-apps/auth](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth)
- **Databricks Apps token auth:** How to connect with tokens — [docs.databricks.com/aws/en/dev-tools/databricks-apps/connect-local](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/connect-local)
- **Databricks OAuth U2M:** Token lifetime is 1 hour, refresh via SDK — [docs.databricks.com/aws/en/dev-tools/auth/oauth-u2m](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-u2m)
- **LLM-API-Key-Proxy:** Open-source proxy with OAuth credential rotation — [github.com/Mirrowel/LLM-API-Key-Proxy](https://github.com/Mirrowel/LLM-API-Key-Proxy)
- **MCP auth in Cursor:** MintMCP guide — [mintmcp.com/blog/securing-servers-with-mcp](https://www.mintmcp.com/blog/securing-servers-with-mcp)
