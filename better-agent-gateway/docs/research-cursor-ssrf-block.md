# Research: Cursor SSRF Block on localhost Override URLs

## The Error

When setting `cursor.general.overrideOpenaiBaseUrl` to `http://127.0.0.1:8787/v1`, Cursor returns:

```json
{"error":{"type":"client","reason":"ssrf_blocked","message":"connection to private IP is blocked","retryable":false}}
```

## Root Cause: Server-Side SSRF Protection

The SSRF block is **server-side**, not client-side. Here is how Cursor's request flow works:

### Request Architecture (from source analysis)

1. **Cursor does NOT call your override URL directly.** It uses a protobuf/gRPC-based protocol to communicate with its backend at `api2.cursor.sh`.

2. **The override URL is sent as metadata** inside a `ModelDetails` protobuf message:
   ```
   aiserver.v1.ModelDetails {
     model_name: string
     api_key: string
     openai_api_base_url: string   // <-- your override URL goes here (field 6)
     ...
   }
   ```

3. **Cursor's server (`api2.cursor.sh`) then makes the actual HTTP call** to the `openai_api_base_url` you provided.

4. **The server has SSRF protection** that blocks requests to private/internal IPs (127.0.0.1, 10.x.x.x, 192.168.x.x, etc.).

### Evidence from Cursor Source Code

**File:** `/Applications/Cursor.app/Contents/Resources/app/extensions/cursor-always-local/dist/main.js`

- **Transport always goes to `api2.cursor.sh`**: The `createTransports()` method sets `baseUrl` from `cursorCreds.backendUrl`, which defaults to `https://api2.cursor.sh`. There is a `replaceBaseUrlWithApi2()` function that explicitly rewrites api3/api4/api5/gcpp URLs back to api2.
- **`_isLocalhostUrl()` exists** but is only used for analytics/logging routing, NOT for AI model requests.
- **`lclhst.build` domain**: Cursor's internal dev domain for local testing. The code checks for this in `backendUrl` (the gRPC transport URL), not in the model override URL.

**File:** `/Applications/Cursor.app/Contents/Resources/app/extensions/cursor-agent-exec/dist/main.js`

- **`ModelDetails` protobuf** includes `openaiApiBaseUrl` as a field that gets serialized and sent to the server. The server receives this URL and makes the outbound HTTP call.

### Why the Error is Server-Side

- The string `ssrf_blocked` does **not appear anywhere** in Cursor's client-side JavaScript (searched all 53MB+ of bundled JS).
- The error JSON format (`{"error":{"type":"client","reason":"ssrf_blocked",...}}`) is a server response, not a client-generated error.
- The `overrideOpenaiBaseUrl` setting name does not appear in any client bundle -- it's read from VS Code settings at runtime and passed as a protobuf field.

## Implications

1. **No client-side workaround exists.** The SSRF check happens on Cursor's servers, not in the Electron app.
2. **localhost/private IPs will never work** with `overrideOpenaiBaseUrl` because Cursor's server won't connect to them.
3. **The override URL MUST be a publicly routable address** that Cursor's servers can reach.

## Workarounds

### 1. Deploy the Gateway to a Public URL (Recommended)
Deploy the gateway as a Databricks App with a public HTTPS endpoint. Use that URL as the override:
```
cursor.general.overrideOpenaiBaseUrl = "https://my-gateway.aws.databricksapps.com/api/v1"
```

### 2. Use a Tunnel (ngrok, Cloudflare Tunnel)
Expose the local proxy via a public tunnel:
```bash
ngrok http 8787
# Then use the ngrok URL: https://abc123.ngrok-free.app/v1
```

### 3. Use Cursor's API Key Field Directly
If the gateway supports token-based auth, configure Cursor with:
- **API Key:** Your Databricks token
- **Override URL:** The public Databricks endpoint directly

### 4. Do NOT Use overrideOpenaiBaseUrl for Local Development
For local testing, use `curl` or the OpenAI Python SDK directly against `http://127.0.0.1:8787/v1`. The Cursor IDE integration requires a publicly accessible URL.

## What `cursor.general.overrideOpenaiBaseUrl` Actually Does

Despite the name suggesting "OpenAI Base URL Override," it does NOT make Cursor call that URL directly. Instead:

1. Cursor reads the setting value from VS Code configuration
2. Cursor includes it in the `ModelDetails.openai_api_base_url` protobuf field
3. Cursor sends the gRPC request to `api2.cursor.sh`
4. Cursor's server extracts the URL and makes the HTTP call on your behalf
5. Cursor's server applies SSRF protection before making the outbound call

This is a **server-side proxy architecture**, not a client-side URL redirect.

## Cursor CLI Analysis

- **Version:** 2.6.19
- **Available commands:** Standard VS Code CLI (diff, merge, goto, etc.) plus `tunnel`, `serve-web`, `agent`
- **`--status` output:** Shows process tree, GPU status, memory -- no AI/model diagnostics
- **No way to trigger model requests via CLI**
- **No diagnostic commands** for AI configuration

## Key File Locations

- Cursor settings: `~/Library/Application Support/Cursor/User/settings.json`
- Cursor app bundle: `/Applications/Cursor.app/Contents/Resources/app/`
- AI transport code: `/Applications/Cursor.app/Contents/Resources/app/extensions/cursor-always-local/dist/main.js`
- Agent execution code: `/Applications/Cursor.app/Contents/Resources/app/extensions/cursor-agent-exec/dist/main.js`
- Product config: `/Applications/Cursor.app/Contents/Resources/app/product.json`
- No client-side logs directory found at standard paths
