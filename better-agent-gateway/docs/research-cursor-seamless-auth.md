# Research: Seamless SSO for Cursor IDE + Databricks Apps Gateway

**Date:** 2026-03-14
**Branch:** `feat/cursor-config-endpoint`
**Status:** Deep research complete. Two viable paths identified.
**Builds on:** `research-cursor-oauth-auth-strategy.md`, `research-local-oauth-proxy.md`

---

## Executive Summary

There is no zero-friction SSO path from Cursor IDE to a Databricks Apps gateway today. Cursor routes all BYOK model requests through its own backend (SSRF blocks private IPs), has no OAuth flow for model endpoints, and stores API keys as static Bearer tokens with no refresh. MCP OAuth exists but is buggy (refresh tokens not used) and MCP is for tools, not model routing.

**Two viable approaches survive scrutiny:**

| Approach | UX Friction | Identity Fidelity | Effort |
|---|---|---|---|
| **A. Gateway-Issued API Keys** (browser SSO -> long-lived key) | Low (one-time browser visit + paste) | Audit-only (gateway SP calls downstream) | 3-5 days |
| **B. Local OAuth Proxy** (localhost process with Databricks SDK auth) | Medium (install + ngrok or tunnel) | Full per-user OBO | 8-12 days |

Both were identified in prior research. This document evaluates 8 additional angles and confirms none of them offer a better path. **Gateway-Issued API Keys remain the recommended approach.**

---

## Angle 1: Cursor MCP Servers for Auth/Token Refresh

### Question
Can an MCP server running locally handle auth and token refresh, sidestepping Cursor's BYOK limitations?

### Findings

**How MCP works in Cursor:**
- Cursor supports stdio (local process) and Streamable HTTP (remote) MCP transports
- Stdio MCP servers run as local child processes on the user's machine -- they are NOT routed through Cursor's backend, so no SSRF restriction applies
- MCP servers expose *tools* that the AI agent can invoke, not model endpoints

**MCP OAuth status in Cursor (as of March 2026):**
- Cursor shipped MCP OAuth support in v1.0 (June 2025) -- browser-based OAuth for remote MCP servers
- **Critical bug:** Cursor stores refresh tokens but does NOT use them ([forum report](https://forum.cursor.com/t/missing-refresh-token-logic-for-mcp-oauth/130765), [another](https://forum.cursor.com/t/cursor-does-not-refresh-oauth-tokens-for-mcp-servers/149511))
- When access tokens expire (~15 min for some providers, 1 hr for Databricks), tool calls fail with 401 and Cursor marks the server as "Logged out"
- User must manually re-authenticate -- no auto-refresh
- Recent bug (Feb 2026): OAuth flow's "Connect" button sometimes produces zero network requests ([forum](https://forum.cursor.com/t/remote-mcp-server-connect-button-produces-zero-network-requests-oauth-flow-never-starts/150962))

**Could MCP proxy model completions?**
- In theory, yes: an MCP tool could accept a prompt and call the gateway's `/v1/chat/completions` endpoint, returning the result as tool output
- Projects like [any-chat-completions-mcp](https://github.com/pyroprompts/any-chat-completions-mcp) and [MCP-Bridge](https://github.com/SecretiveShell/MCP-Bridge) do exactly this
- **But this is a terrible UX for coding assistance.** MCP tools return structured data to the agent, not streaming chat responses. The model would need to invoke a tool, wait for the full response, then relay it. No streaming, no inline code generation, no diff application. It fundamentally breaks Cursor's coding workflow.

### Verdict: NOT VIABLE for model routing

MCP is architecturally wrong for this use case. It's for tools (search, file ops, API calls), not for replacing the model provider. And even if you hacked around that, Cursor's MCP OAuth is broken (no token refresh), which defeats the purpose.

**One legitimate use:** A *local stdio* MCP server could provide Databricks-specific tools (run SQL, read notebooks) alongside the gateway-as-model-provider approach. Auth would be handled by the local MCP server process using `databricks-sdk`, completely bypassing Cursor's broken MCP OAuth. This is orthogonal to the model auth problem.

---

## Angle 2: Cursor/VS Code Extensions for Token Injection

### Question
Can a VS Code/Cursor extension intercept outbound model API requests and inject a fresh Bearer token?

### Findings

**Extension API limitations:**
- VS Code's extension API does not expose a hook to intercept or modify outbound HTTP requests made by the host application
- Extensions can make their own HTTP requests, but cannot middleware Cursor's internal model calls
- Cursor's model request pipeline is internal to the Electron app -- it's not extensible
- There is no `onBeforeRequest` or request interceptor API in the VS Code extension model

**What extensions CAN do:**
- Register custom commands, views, tree providers
- Use `vscode.workspace.getConfiguration()` to read/write VS Code settings
- Access the terminal, file system, language servers
- **Cannot** modify Cursor Settings (model config is in SQLite/Keychain, not `settings.json`)

**Cursor-specific extension hooks:**
- Cursor does not expose any extension API beyond what VS Code provides
- No Cursor-specific extension points for auth, model routing, or request modification
- The Cursor Rules feature (`.cursor/rules/`) is for prompt engineering, not auth

### Verdict: NOT VIABLE

Extensions cannot intercept Cursor's model API calls. The HTTP pipeline is internal and not extensible.

---

## Angle 3: OAuth Refresh Token Flow (Gateway-Side Token Exchange)

### Question
Could the user paste a Databricks refresh token into Cursor's API key field, and the gateway exchanges it for an access token on each request?

### Findings

**How Databricks refresh tokens work:**
- `databricks auth login` stores tokens at `~/.databricks/token-cache.json`
- Contains: access token, refresh token, expiry timestamp
- Access tokens expire in 1 hour; refresh tokens are longer-lived (days to weeks)
- Databricks supports single-use refresh tokens (rotate on each use) for enhanced security

**The proposed flow:**
1. User runs `databricks auth login` (one-time browser SSO)
2. User copies the refresh token from `~/.databricks/token-cache.json`
3. Pastes it into Cursor's "OpenAI API Key" field
4. Gateway receives `Authorization: Bearer <refresh_token>`
5. Gateway exchanges refresh token for access token via Databricks OAuth token endpoint
6. Gateway uses the access token to call downstream APIs on behalf of the user

**Why this is problematic:**

| Issue | Severity |
|---|---|
| Single-use refresh tokens: after one exchange, the token is invalidated and a NEW refresh token is issued. The gateway would need to communicate the new refresh token back to Cursor -- impossible since Cursor doesn't read response headers for auth updates. | **Fatal** |
| Even without single-use tokens, the refresh token in Cursor's API key field becomes stale after first gateway use | **Fatal** |
| Storing a refresh token as a "static API key" violates the token's security model -- refresh tokens are meant to be client-side secrets, not transmitted on every request | **High** |
| Databricks token endpoint requires client_id and redirect_uri for the exchange, which the gateway would need to hardcode or impersonate | **High** |
| If single-use tokens are disabled, the refresh token IS long-lived enough (~days), but sending it on every request massively increases exposure | **Medium** |

**Could the gateway cache?**
- Gateway receives refresh token once, exchanges it, caches the access token, and uses a session ID going forward
- This is essentially "Gateway-Issued API Keys" (Approach 4 from prior research) with extra steps
- The refresh token exchange happens once at "registration time" -- then you're back to the session token pattern
- Adds complexity with no benefit over generating a gateway API key directly

### Verdict: NOT VIABLE as described

The single-use refresh token rotation makes this fundamentally incompatible with a static API key field. Even without single-use tokens, the security model is wrong (refresh tokens shouldn't be transmitted on every HTTP request). The "cache and session" variant collapses into Gateway-Issued API Keys.

---

## Angle 4: Cursor's API Key Field as Session Identifier

### Question
Can we abuse Cursor's API key field to pass a session ID that the gateway maps to a cached OBO token?

### Findings

**How Cursor sends the API key:**
- Cursor sends `Authorization: Bearer <apiKey>` on every request to the overridden base URL
- The API key is a static string set in the GUI -- it does not change between requests
- Cursor does NOT send any other identifying headers (no session cookies, no client ID)
- All requests are routed through Cursor's backend, which adds its own headers but strips/replaces auth

**This IS the Gateway-Issued API Keys approach:**
- The "session identifier" in the API key field is exactly what Approach 4 (from prior research) proposes
- User visits gateway in browser -> SSO -> gets a `bag-xxxxxxxxxxxx` key -> pastes into Cursor
- Gateway maps `bag-xxxxxxxxxxxx` to a user record on each request
- This is the recommended approach and it works

**Can the gateway also cache an OBO token?**
- During the browser session where the key is generated, the gateway has access to the user's OBO token via `x-forwarded-access-token`
- Gateway could cache this token alongside the API key mapping
- OBO token lasts ~1 hour, then expires with no refresh (Databricks Apps proxy handles OAuth, app doesn't get a refresh token)
- After 1 hour, gateway must fall back to its own service principal
- Net benefit: ~1 hour of per-user identity, then same as plain API key approach

### Verdict: VIABLE (this IS the recommended approach)

This angle confirms rather than replaces the Gateway-Issued API Keys strategy. The OBO token caching adds marginal value (1 hour of per-user identity) at added complexity -- not worth it for MVP.

---

## Angle 5: OAuth Device Authorization Flow

### Question
Like GitHub CLI's device flow -- user runs a command, gets a code, enters it in browser, gateway issues a session token. Applicable here?

### Findings

**Device Authorization Grant (RFC 8628):**
- Designed for input-constrained devices (smart TVs, CLI tools)
- Flow: device requests code -> user enters code in browser -> device polls for token
- Used by: GitHub CLI, Docker CLI, AWS SSO, Azure CLI

**Databricks support:**
- Databricks does NOT implement the device authorization grant endpoint
- Databricks OAuth supports: authorization code + PKCE (U2M), client credentials (M2M), token federation
- No `/device/code` or `/device/token` endpoints exist

**Could the gateway implement its own device flow?**
- Yes, the gateway could act as its own authorization server with device flow
- Implementation:
  1. `POST /device/code` -> returns `{ device_code, user_code, verification_uri }`
  2. User visits `verification_uri` in browser -> Databricks SSO -> enters user code
  3. Gateway associates the device code with the authenticated user
  4. CLI/script polls `POST /device/token` until complete -> gets a gateway session token
  5. User pastes session token into Cursor
- This is essentially Gateway-Issued API Keys with a CLI-friendly issuance flow instead of browser-only

**Prior art:**
- [LLM-API-Key-Proxy](https://github.com/Mirrowel/LLM-API-Key-Proxy) uses OAuth Device Flow for Qwen/Dashscope
- Auth0 has a [complete guide](https://auth0.com/docs/get-started/authentication-and-authorization-flow/device-authorization-flow/call-your-api-using-the-device-authorization-flow) on implementing device flow

**Benefit over browser-only key generation:**
- Could be scripted (no manual browser navigation to the gateway)
- CLI-friendly: `curl POST /device/code` -> open URL -> `curl POST /device/token`
- Better for automation, team onboarding scripts

### Verdict: PROMISING as enhancement to Gateway-Issued API Keys

Not a standalone solution, but a nice alternative issuance mechanism. The end result is the same -- a long-lived session token in Cursor's API key field. Adding a device flow endpoint to the gateway would let teams script onboarding:

```bash
# Hypothetical CLI flow
bag_token=$(curl -s https://gateway.databricksapps.com/device/code | jq -r .user_code)
echo "Visit https://gateway.databricksapps.com/device/verify and enter: $bag_token"
# ... user authenticates in browser ...
api_key=$(curl -s https://gateway.databricksapps.com/device/token -d "device_code=$device_code" | jq -r .api_key)
echo "Paste this into Cursor: $api_key"
```

**Effort:** +1-2 days on top of Gateway-Issued API Keys base implementation.

---

## Angle 6: Cloudflare Access / Tunnel with Identity

### Question
Could a Cloudflare tunnel in front of the gateway handle SSO and inject identity headers?

### Findings

**How Cloudflare Access works:**
- Cloudflare Access is a reverse proxy that authenticates users before forwarding to your origin
- For browser users: redirects to IdP, sets a `CF_Authorization` cookie
- For non-browser clients: requires `CF-Access-Client-Id` + `CF-Access-Client-Secret` headers (service tokens)
- On successful auth, Cloudflare injects `Cf-Access-Jwt-Assertion` header with user identity

**Problem: Cursor is a non-browser client**
- Cursor sends `Authorization: Bearer <apiKey>` -- it cannot send Cloudflare service token headers
- Cursor does not follow redirects to an IdP login page
- Cursor cannot present a `CF_Authorization` cookie
- No way to configure Cursor to send `CF-Access-Client-Id` / `CF-Access-Client-Secret` headers

**Could we use the Bearer token AS a Cloudflare service token?**
- No. Cloudflare requires TWO headers (`CF-Access-Client-Id` AND `CF-Access-Client-Secret`). Cursor sends one (`Authorization: Bearer`).
- Even if we could hack around this, Cloudflare service tokens are per-service, not per-user. They don't carry user identity.

**Could Cloudflare Tunnel help with the local proxy approach?**
- A Cloudflare Tunnel could replace ngrok for the local proxy, making `localhost` reachable via a `*.trycloudflare.com` URL
- But this adds another dependency (cloudflared) and Cloudflare account
- Doesn't solve the core auth problem

**Architecture conflict:**
- The gateway is already a Databricks App with its own auth (SSO via Databricks Apps proxy)
- Adding Cloudflare Access in front would create competing auth layers
- The Databricks Apps proxy already handles SSO -- Cloudflare would be redundant for browser users

### Verdict: NOT VIABLE

Cursor cannot authenticate with Cloudflare Access (wrong headers, no cookie support). Cloudflare doesn't add value on top of Databricks Apps' built-in auth. The local proxy + tunnel variant adds complexity without solving the core problem.

---

## Angle 7: How Other AI IDEs Handle Custom Auth

### Cody (Sourcegraph)

**Architecture:** Server-mediated. All requests go through a Sourcegraph Enterprise server.
- SSO via SAML, OIDC, or OAuth at the Sourcegraph server level
- IDE extension authenticates to Sourcegraph, not directly to the LLM provider
- Sourcegraph server calls LLM APIs with its own credentials (BYOK at server level, not client level)
- Per-user identity flows through Sourcegraph's auth, not the LLM provider's

**Relevance:** This is essentially the Gateway-Issued API Keys pattern. Sourcegraph server = our gateway. The IDE authenticates to the intermediary, not the LLM. Validates our approach.

### Continue.dev

**Current state:**
- Supports `apiKey` (static Bearer token) and custom headers via `requestOptions.headers`
- OAuth support is [requested](https://github.com/continuedev/continue/issues/1173) but NOT implemented for model providers
- MCP OAuth is [in progress](https://github.com/continuedev/continue/issues/6282) but not shipped
- There's an [open issue](https://github.com/continuedev/continue/issues/7201) for OAuth-proxy support

**Relevance:** Continue has the same limitation as Cursor -- static API keys only for model providers. Our gateway approach works for Continue too (same OpenAI-compatible key + base URL pattern).

### Tabby

**Architecture:** Fully self-hosted.
- Server runs on your infrastructure with enterprise SSO (LDAP, GitLab SSO)
- IDE extensions authenticate to the Tabby server
- Server handles LLM inference locally or via configured backends
- OpenAPI interface for integration

**Relevance:** Another server-mediated pattern. Validates that "authenticate to the intermediary" is the industry standard when SSO is required.

### Pattern Summary

Every IDE that supports enterprise SSO does it the same way:
1. A server/gateway sits between the IDE and the LLM provider
2. The IDE authenticates to the gateway (usually with a static token or API key)
3. The gateway handles SSO and calls the LLM with its own credentials
4. Per-user identity is tracked at the gateway level

**No AI IDE supports custom OAuth flows for direct-to-model-provider authentication.** Gateway-mediated auth is the universal pattern.

---

## Angle 8: Cursor Changelog / Roadmap

### Recent relevant changes (2025-2026)

| Date | Feature | Relevance |
|---|---|---|
| June 2025 | MCP OAuth support (v1.0) | First IDE with browser-based MCP auth. Buggy (no refresh). |
| Jan 2026 | Cursor CLI + one-click MCP auth | CLI support for MCP, improved MCP auth UX |
| Jan 2026 | Agent modes in CLI (Plan, Ask) | CLI getting closer to IDE parity |
| Feb 2026 | Multiple MCP OAuth bugs reported | Connect button broken, refresh tokens ignored |
| March 2026 | Override Base URL still global | No per-model base URL support |

### What's NOT on the roadmap (as visible from forums/changelog)

- No custom OAuth for model providers (only MCP)
- No credential helpers (like git's `credential.helper`)
- No per-model base URL override
- No extension API for auth middleware
- No plans to allow localhost connections without tunnel (SSRF protection is intentional)

### BYOK Behavior Clarification

Critical finding from deeper research:
- **All BYOK requests are routed through Cursor's backend** for prompt construction
- Even with "Override OpenAI Base URL", Cursor's server makes the final call to your endpoint
- This is why `localhost` doesn't work -- Cursor's server can't reach your machine
- The SSRF block isn't a bug, it's a security feature of their architecture
- This means ngrok/tunnel is REQUIRED for the local proxy approach

Source: [forum.cursor.com](https://forum.cursor.com/t/unable-to-reach-localhost-custom-models/66440), [thebizaihub.com](https://thebizaihub.com/how-to-use-local-models-with-cursor-ai/)

---

## Revised Assessment of All Approaches

### Approach A: Gateway-Issued API Keys (RECOMMENDED)

**Status:** Confirmed viable. Industry-standard pattern (Cody, Tabby do the same thing).

**Flow:**
1. User visits `https://gateway.databricksapps.com/` in browser
2. Databricks SSO happens automatically
3. User clicks "Generate Cursor API Key" -> gets `bag-xxxxxxxxxxxx`
4. User pastes key + base URL into Cursor settings
5. Gateway maps key to user identity on each request
6. Gateway calls downstream APIs with its own service principal

**Why it wins:**
- Zero install, works today
- Static key is exactly what Cursor expects
- Long-lived (30-90 days, configurable)
- Revocable, auditable
- Gateway is publicly accessible (no localhost/SSRF issue)
- Works with ALL clients (Cursor, Continue, any OpenAI-compatible tool)

**What's lost:**
- Per-user OAuth scoping at the Databricks API level (gateway SP acts on behalf of all users)
- Must implement gateway-level permissions and audit logging

**Enhancement: Device flow for CLI issuance** (+1-2 days)
- `POST /device/code` -> user code + verification URL
- User authenticates in browser -> gateway issues API key
- Enables scripted team onboarding

### Approach B: Local OAuth Proxy + Tunnel (RUNNER-UP)

**Status:** Viable but higher friction than previously assessed.

**Updated understanding:** Because Cursor routes BYOK requests through its backend, the local proxy MUST be exposed via a public tunnel (ngrok, Cloudflare Tunnel, or similar). This significantly changes the effort and UX:

| Factor | Previous Assessment | Updated Assessment |
|---|---|---|
| Localhost works directly | Maybe (unclear) | **No** -- ngrok required |
| Install complexity | `uvx` one-liner | `uvx` + ngrok account + ngrok setup |
| Running services | 1 (proxy) | 2 (proxy + ngrok) |
| Security surface | Local only | Public URL (tunnel) |
| Cost | Free | ngrok free tier has limits; paid for custom domains |

**When this is still worth it:**
- You need *true* per-user OAuth identity flowing to downstream Databricks APIs
- You have compliance requirements that prevent a shared service principal
- Users are technical enough to manage a local proxy + tunnel

### Why Not Both?

These approaches are complementary, not exclusive:
- **API keys** for quick setup, demos, light use
- **Local proxy** for power users who need per-user identity or work with sensitive data

The gateway auth middleware can support both:
```
Authorization: Bearer bag-xxxx   -> API key lookup -> SP credentials
Authorization: Bearer eyJ...     -> Databricks OAuth token -> OBO passthrough
```

---

## Implementation Recommendations

### Phase 1: Gateway-Issued API Keys (3-5 days)

1. **Key storage** -- hash-only storage in gateway (SQLite or Databricks SQL table)
2. **Browser UI** -- key generation page behind Databricks Apps SSO
3. **Auth middleware** -- `bag-` prefix detection + lookup in `server/auth.py`
4. **Key management** -- revoke, list, rotate via UI
5. **Audit logging** -- every request logged with user identity from key mapping

### Phase 2 (optional): Device Flow for CLI Onboarding (+1-2 days)

1. `POST /device/code` endpoint
2. `GET /device/verify` browser page (SSO-protected)
3. `POST /device/token` polling endpoint
4. Enables scripted onboarding: `curl ... | jq` workflows

### Phase 3 (optional): Local Proxy Package (+8-12 days)

1. PyPI package `databricks-cursor-proxy`
2. OAuth via `databricks-sdk`
3. ngrok integration or Cloudflare Tunnel instructions
4. Cross-platform service management

---

## Key Takeaways

1. **No AI IDE supports custom OAuth for model providers.** Every enterprise solution uses a gateway/intermediary with static tokens. This isn't a gap in our research -- it's the state of the industry.

2. **Cursor's SSRF protection is architectural, not a bug.** All BYOK requests go through Cursor's backend. Localhost will never work without a tunnel. Plan accordingly.

3. **Cursor's MCP OAuth is broken** (no token refresh). Don't build auth flows that depend on it.

4. **Gateway-Issued API Keys is the Cody/Tabby/LiteLLM pattern.** It's battle-tested across the industry. The trade-off (per-user identity at gateway level, not Databricks API level) is the same trade-off everyone makes.

5. **Device flow is a nice enhancement** for CLI-friendly key issuance, worth the +1-2 day investment.

---

## Sources

### Cursor Auth Behavior
- [Cursor MCP docs](https://docs.cursor.com/context/model-context-protocol)
- [Cursor changelog](https://cursor.com/changelog)
- [MCP OAuth refresh bug](https://forum.cursor.com/t/missing-refresh-token-logic-for-mcp-oauth/130765)
- [MCP OAuth refresh bug (2)](https://forum.cursor.com/t/cursor-does-not-refresh-oauth-tokens-for-mcp-servers/149511)
- [OAuth Connect button broken](https://forum.cursor.com/t/remote-mcp-server-connect-button-produces-zero-network-requests-oauth-flow-never-starts/150962)
- [Localhost blocked](https://forum.cursor.com/t/unable-to-reach-localhost-custom-models/66440)
- [BYOK routing through Cursor servers](https://forum.cursor.com/t/how-to-set-local-private-model-in-cursor-ide-cli/149283)
- [Base URL override bugs](https://forum.cursor.com/t/cursor-models-fail-when-using-byok-openai-key-with-overridden-base-url-glm-4-7/147218)
- [BYOK v2.4.22 issues](https://forum.cursor.com/t/not-work-with-byok-with-cursor-2-4-22-version/150158)
- [Custom endpoint feature request](https://forum.cursor.com/t/custom-endpoint-and-api-key-support/129424)
- [Cursor CLI Jan 2026](https://forum.cursor.com/t/cursor-cli-jan-16-2026/149172)

### MCP and Tools
- [MCP auth guide (TrueFoundry)](https://www.truefoundry.com/blog/mcp-authentication-in-cursor-oauth-api-keys-and-secure-configuration)
- [mcp-remote npm](https://www.npmjs.com/package/mcp-remote)
- [MCP-Bridge (OpenAI-compatible MCP)](https://github.com/SecretiveShell/MCP-Bridge)
- [any-chat-completions-mcp](https://github.com/pyroprompts/any-chat-completions-mcp)

### Databricks OAuth
- [Databricks Apps auth](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth)
- [Databricks Apps token auth](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/connect-local)
- [Databricks OAuth U2M](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-u2m)
- [Single-use refresh tokens](https://docs.databricks.com/aws/en/integrations/single-use-tokens)

### Other IDEs
- [Cody enterprise auth](https://sourcegraph.com/docs/cody/clients/enable-cody-enterprise)
- [Cody Gateway](https://sourcegraph.com/docs/cody/core-concepts/cody-gateway)
- [Continue OAuth issue](https://github.com/continuedev/continue/issues/1173)
- [Continue MCP OAuth issue](https://github.com/continuedev/continue/issues/6282)
- [Tabby self-hosted](https://github.com/TabbyML/tabby)

### LiteLLM / Proxy Patterns
- [LiteLLM Cursor integration](https://docs.litellm.ai/docs/tutorials/cursor_integration)
- [LiteLLM virtual keys](https://docs.litellm.ai/docs/proxy/virtual_keys)
- [LiteLLM custom auth](https://docs.litellm.ai/docs/proxy/custom_auth)
- [LLM-API-Key-Proxy](https://github.com/Mirrowel/LLM-API-Key-Proxy)

### Cloudflare
- [Cloudflare Access service tokens](https://developers.cloudflare.com/cloudflare-one/access-controls/service-credentials/service-tokens/)
- [Cf-Access-Jwt-Assertion](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/authorization-cookie/application-token/)

### Device Flow
- [Auth0 device flow guide](https://auth0.com/docs/get-started/authentication-and-authorization-flow/device-authorization-flow/call-your-api-using-the-device-authorization-flow)

### Local Model Guides (confirm SSRF behavior)
- [Local models with Cursor 2026](https://thebizaihub.com/how-to-use-local-models-with-cursor-ai/)
- [Ollama + Cursor + ngrok](https://themeansquare.medium.com/running-local-ai-models-in-cursor-the-complete-guide-4290fe0383fa)
