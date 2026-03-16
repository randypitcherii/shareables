# Research: Cursor SSRF Block on Localhost Base URL

## The Problem
Setting `cursor.general.overrideOpenaiBaseUrl` to `http://127.0.0.1:8787/v1` returns:
```json
{"error":{"type":"client","reason":"ssrf_blocked","message":"connection to private IP is blocked","retryable":false}}
```

## Root Cause: Cursor's Architecture

**This is by design, not a bug.** When you use "Override OpenAI Base URL" with BYOK (Bring Your Own Key), the request flow is:

```
Your Machine → Cursor Servers → [Your Base URL Endpoint]
```

Cursor proxies BYOK requests through their own infrastructure. Their servers have SSRF protection that blocks connections to private/internal IP ranges (127.0.0.0/8, 10.0.0.0/8, 192.168.0.0/16, etc.).

This is confirmed by Cursor staff on their forum:
> "When you configure [custom models] in Cursor with BYOK, the request flow is: Your Machine → Cursor Servers → [Endpoint]. Because requests are proxied through Cursor's infrastructure, we have SSRF protection that blocks connections to private/internal IP ranges."

Source: https://forum.cursor.com/t/cursor-can-not-work-with-azure-openai/148758

## How Ollama/LiteLLM Users Actually Work Around This

Every working Ollama + Cursor setup uses **ngrok or a similar public tunnel**. There is no secret setting that bypasses the SSRF check. The pattern is:

1. Run Ollama/LiteLLM locally
2. Expose it via **ngrok** (or Cloudflare Tunnel, No-IP + port forwarding, etc.)
3. Set the **ngrok public HTTPS URL** as the Override OpenAI Base URL
4. Use any string as the API key (e.g., "ollama")

Example flow:
```
Cursor IDE → Cursor Servers → ngrok public URL → ngrok tunnel → localhost:11434
```

### LiteLLM Specific Setup (from official docs)
- Base URL: `https://<your-litellm-proxy>/cursor` (must be publicly reachable)
- API Key: LiteLLM virtual key
- Source: https://docs.litellm.ai/docs/tutorials/cursor_integration

### Cursor Staff Confirmation
> "Right now, Cursor supports the 'Override OpenAI Base URL' option, but it requires a publicly accessible HTTPS endpoint. Direct connections to localhost or a LAN IP aren't supported yet."

Source: https://forum.cursor.com/t/add-an-option-to-add-local-model-in-the-same-machine-or-lan-with-just-the-ip-and-http/148311

## What This Means For Our Gateway

Our gateway at `localhost:8787` **cannot** be used directly as Cursor's Override OpenAI Base URL. The request never reaches our server -- it's blocked at Cursor's proxy layer before it even tries to connect.

### Options

1. **Public tunnel (ngrok/Cloudflare Tunnel)**: Expose localhost:8787 via a tunnel, use the public URL in Cursor. This works but defeats the "local gateway" purpose and adds latency.

2. **Cursor's `/config` endpoint approach**: Instead of overriding the base URL, investigate whether Cursor has a configuration endpoint that can be served to redirect traffic differently. This is what the `feat/cursor-config-endpoint` branch seems to be exploring.

3. **Local proxy interception**: One Reddit project (multi_ai_proxy) uses a local proxy that intercepts Cursor's outbound OpenAI requests before they leave the machine. This requires:
   - Initial verification with a real OpenAI API key
   - Then switching the base URL to `http://localhost:...`
   - This apparently works because after initial verification, some request paths may be handled client-side

4. **Wait for Cursor to add local model support**: There's an open feature request for direct localhost connections. Not available yet as of March 2026.

## Key Insight

The fundamental issue: **Cursor's BYOK requests are server-side proxied, not client-side.** Other IDEs (Roo Code, Kilo Code, Continue, etc.) make direct connections from your machine, so localhost works fine. Cursor's architecture routes through their servers, making localhost unreachable.

The only confirmed working patterns for local endpoints require a publicly routable URL via ngrok or similar tunneling.
