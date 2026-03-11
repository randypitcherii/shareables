# Databricks Apps Programmatic Authentication Research

**Date:** 2026-03-11
**Context:** Better Agent Gateway deployed as a Databricks App with `user_api_scopes: [serving.serving-endpoints]`

---

## 1. What auth mechanism does the Databricks Apps proxy use?

**Cookie-based sessions.** After a user completes the browser OAuth flow, the Databricks Apps proxy sets a `__Host-databricksapps` session cookie.

- **Cookie format:** `__Host-databricksapps=<encrypted_session_data>|<timestamp>|<signature>`
- **TTL:** Tied to the Databricks SSO session (no explicit docs on exact TTL, but empirically ~1 hour matching the OAuth token lifetime, with potential refresh behavior)
- **Behavior:** The proxy intercepts every request to the app URL. If the cookie is present and valid, the proxy injects `X-Forwarded-Access-Token`, `X-Forwarded-User`, and `X-Forwarded-Email` headers into the request before forwarding to the app container. If the cookie is missing/expired, the proxy redirects to the Databricks SSO login page.

**Source:** [smithery.ai/skills/databricks-apps-cookie-auth](https://smithery.ai/skills/hurtener/databricks-apps-cookie-auth)

---

## 2. Can a Databricks SDK OAuth token be used directly via `Authorization: Bearer`?

**YES -- but ONLY for `/api/` routes.** This is a relatively new feature documented by Databricks.

### Key constraints:
- The app **must expose endpoints under `/api/` path prefix** -- token auth does NOT work for UI-only apps
- The token must include the **scopes matching the app's user authorization config** (e.g., `serving.serving-endpoints`)
- PATs (Personal Access Tokens) do **NOT** work -- always returns 401
- Token lifetime is 1 hour (standard OAuth)

### How to get a valid token:

**Option A: Local dev (U2M)**
```bash
databricks auth login --host https://<workspace-url> --profile my-env
databricks auth token --profile my-env
```
Or in Python:
```python
from databricks.sdk.core import Config
config = Config(profile="my-env")
token = config.oauth_token().access_token
```

**Option B: Service Principal (M2M) -- for automation**
```python
from databricks.sdk import WorkspaceClient
wc = WorkspaceClient(
    host="https://<workspace-url>",
    client_id="<sp-client-id>",
    client_secret="<sp-client-secret>"
)
headers = wc.config.authenticate()
# headers dict contains {"Authorization": "Bearer <token>"}
```

**Option C: curl with token**
```bash
TOKEN=$(databricks auth token --profile my-env | jq -r '.access_token')
curl -H "Authorization: Bearer $TOKEN" \
  "https://<app-url>/api/v1/chat/completions" \
  -d '{"model": "claude-sonnet-latest", "messages": [...]}'
```

### Scope requirement:
When using the CLI/SDK with unified auth, tools automatically request the `all-apis` scope. However, if the app has user authorization scopes configured, you **must manually request matching scopes** via a custom OAuth flow:

```bash
curl --request POST \
  https://<workspace-url>/oidc/v1/token \
  --data "client_id=databricks-cli" \
  --data "grant_type=authorization_code" \
  --data "scope=serving.serving-endpoints" \
  --data "redirect_uri=<redirect-url>" \
  --data "code_verifier=<verifier>" \
  --data "code=<auth-code>"
```

If the token lacks the required scopes, the app returns **401 or 403**.

**Source:** [docs.databricks.com/dev-tools/databricks-apps/connect-local](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/connect-local)

---

## 3. What is the `.auth/callback` endpoint?

The `/.auth/callback` endpoint is the **OAuth redirect URI** used by the Databricks Apps proxy during the browser-based OAuth consent flow. It is NOT part of your app code -- it's handled entirely by the proxy.

**Flow:**
1. User visits `https://<app-url>/`
2. Proxy redirects to `https://<workspace-url>/oidc/v1/authorize?...&redirect_uri=https://<app-url>/.auth/callback`
3. User authenticates via SSO and consents to the requested scopes
4. Databricks redirects back to `https://<app-url>/.auth/callback?code=<auth-code>&state=<state>`
5. The proxy at `/.auth/callback` exchanges the code for tokens and **sets the `__Host-databricksapps` session cookie**
6. Proxy redirects the user to the original URL they requested

**Can you call it programmatically?** In theory, you could follow the full PKCE flow with httpx (generate verifier/challenge, hit the authorize endpoint, follow redirects through SSO, capture the callback). In practice, this is fragile because:
- SSO login pages vary by IdP and may require browser-like behavior (JavaScript, SAML posts)
- The `/.auth/callback` endpoint expects the proxy's own state parameter, not one you generated
- It's simpler to use the SDK's token-based auth for `/api/` routes

---

## 4. Can you get a session cookie programmatically?

**Technically possible but impractical.** You would need to:
1. Follow the OAuth redirect chain through your IdP's SSO (which often requires JavaScript rendering)
2. Complete the login (username/password, MFA, etc.)
3. Follow the redirect back to `/.auth/callback`
4. Capture the `__Host-databricksapps` cookie from the Set-Cookie response header

**Better approach:** Use `Authorization: Bearer <token>` on `/api/` routes instead. This completely bypasses the cookie/session mechanism.

If you absolutely need cookie-based access (e.g., for non-`/api/` routes or UI scraping), the Smithery community has documented a workaround: complete browser login once, export the cookie value, and pass it in subsequent requests:

```python
import httpx
resp = httpx.get(
    "https://<app-url>/api/v1/...",
    cookies={"__Host-databricksapps": "<cookie-value>"}
)
```

---

## 5. Can the app's OBO scope be obtained via the Databricks SDK directly?

**Partially.** The standard `databricks auth login` and SDK unified auth request `all-apis` scope by default. The docs explicitly state:

> "CLI and SDK tools only provide basic scopes like `all-apis`, which might not be sufficient for user authorization."

For apps with specific scopes like `serving.serving-endpoints`, you need a **custom OAuth flow** where you explicitly request those scopes in the token request. The SDK's `config.authenticate()` may or may not include the right scopes depending on how the workspace validates them.

**Recommendation:** Test whether `all-apis` scope tokens are accepted by your app's `/api/` routes. If they are, the standard SDK flow works. If not, you need the manual U2M PKCE flow with explicit scope parameters.

---

## Summary: Recommended Programmatic Access Pattern

| Method | Works? | Notes |
|--------|--------|-------|
| Browser cookie (`__Host-databricksapps`) | Yes | Requires browser login, cookie extraction |
| `Authorization: Bearer <OAuth-U2M-token>` on `/api/` routes | **Yes** | Best for interactive dev. Requires correct scopes. |
| `Authorization: Bearer <OAuth-M2M-token>` on `/api/` routes | **Yes** | Best for automation. Service principal + client credentials. |
| PAT (Personal Access Token) | **No** | Always returns 401 on Databricks Apps |
| Programmatic OAuth redirect flow | Fragile | SSO pages vary, impractical to automate |

### For Better Agent Gateway specifically:

Since all endpoints are under `/api/v1/`, token-based auth should work. The recommended approach:

```python
from databricks.sdk import WorkspaceClient

# For user identity (interactive)
wc = WorkspaceClient(host="https://<workspace-url>", profile="my-env")

# For service principal (automation)
wc = WorkspaceClient(
    host="https://<workspace-url>",
    client_id="<sp-client-id>",
    client_secret="<sp-client-secret>"
)

headers = wc.config.authenticate()
# Use headers with httpx/requests to call the app
```

**Tested and confirmed (2026-03-11):** Standard `all-apis` scoped tokens from `databricks auth login` ARE accepted by the app proxy on `/api/` routes. However, the proxy **downscopes the OBO token** to only the declared `user_api_scopes` (`serving.serving-endpoints`). This means:

- Bearer token with `all-apis` → proxy accepts it → OBO token only has `serving.serving-endpoints`
- Catalogs: blocked (`"does not have required scopes: unity-catalog"`)
- Warehouses: blocked (`"does not have required scopes: sql"`)
- Serving endpoints: works (33 visible)

**The three-layer security model is validated:**
1. **SSO-only auth** — PATs always return 401
2. **Proxy-enforced scope downscoping** — OBO token constrained regardless of input token scope
3. **App-level routing** — only proxies to `/serving-endpoints/*/invocations`

Token exchange (RFC 8693) also works for pre-downscoping if desired, but the proxy handles it automatically.
