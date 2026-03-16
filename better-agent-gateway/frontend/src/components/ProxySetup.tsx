import { useEffect, useState } from 'react'

interface ToolConfig {
  name: string
  description: string
  env_vars: Record<string, string>
  config_file?: string
}

interface SetupInfo {
  gateway_url: string
  uvx_from_spec: string
  setup_command: string
  start_command: string
  status_command: string
  health_url: string
  cursor_base_url: string
  version: string
  git_hash: string
  model_aliases: string[]
  tool_configs: Record<string, ToolConfig>
}

const TOOL_ICONS: Record<string, string> = {
  'claude-code': '\u2728',  // sparkles
  'codex': '\u{1F4AC}',     // speech balloon
  'opencode': '\u{1F310}',  // globe
}

type ToolTab = 'claude-code' | 'codex' | 'opencode'

export function ProxySetup() {
  const [info, setInfo] = useState<SetupInfo | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [copied, setCopied] = useState<string | null>(null)
  const [tab, setTab] = useState<ToolTab>('claude-code')

  useEffect(() => {
    fetch('/api/v1/proxy-setup')
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then(setInfo)
      .catch(e => setError(e.message))
  }, [])

  const copyToClipboard = (text: string, label: string) => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(label)
      setTimeout(() => setCopied(null), 2000)
    })
  }

  if (error) return <div className="error">Failed to load setup info: {error}</div>
  if (!info) return <div className="proxy-setup loading">Loading setup info...</div>

  const universalPrompt = buildUniversalAgentPrompt(info)

  return (
    <section className="proxy-setup">
      <h2>Connect Your AI Coding Tool</h2>
      <p className="proxy-description">
        Use the <strong>databricks-agent-proxy</strong> to connect your AI coding tool
        to this gateway. The proxy runs on localhost, handles OAuth automatically, and forwards requests here.
      </p>

      <div className="agent-prompt-cta">
        <button
          className="agent-prompt-btn"
          onClick={() => copyToClipboard(universalPrompt, 'universal')}
        >
          {copied === 'universal' ? 'Copied!' : 'Copy setup prompt for your agent'}
        </button>
        <span className="agent-prompt-hint">
          Paste into any AI coding agent — it will ask which tools to configure and handle the rest
        </span>
      </div>

      <p className="manual-steps-label">Or follow these steps manually for a specific tool:</p>

      <div className="setup-tabs">
        {(['claude-code', 'codex', 'opencode'] as ToolTab[]).map(t => {
          const configKey = t === 'claude-code' ? 'claude_code' : t
          const name = info.tool_configs?.[configKey]?.name ?? t
          return (
            <button
              key={t}
              className={`setup-tab ${tab === t ? 'active' : ''}`}
              onClick={() => setTab(t)}
            >
              <span className="tab-icon">{TOOL_ICONS[t]}</span> {name}
            </button>
          )
        })}
      </div>

      <ToolSetupTab
        tab={tab}
        info={info}
        copied={copied}
        copyToClipboard={copyToClipboard}
      />
    </section>
  )
}

function ToolSetupTab({
  tab,
  info,
  copied,
  copyToClipboard,
}: {
  tab: ToolTab
  info: SetupInfo
  copied: string | null
  copyToClipboard: (text: string, label: string) => void
}) {
  const configKey = tab === 'claude-code' ? 'claude_code' : tab
  const toolConfig = info.tool_configs?.[configKey]

  const envVarSnippet = toolConfig?.env_vars
    ? Object.entries(toolConfig.env_vars)
        .map(([k, v]) => `export ${k}=${v}`)
        .join('\n')
    : ''

  return (
    <div className="setup-tab-content">
      <div className="setup-steps">
        <div className="setup-step">
          <div className="step-number">1</div>
          <div className="step-content">
            <div className="step-label">Install &amp; authenticate the proxy</div>
            <div className="command-block">
              <code>{info.setup_command}</code>
              <button
                className="copy-btn"
                onClick={() => copyToClipboard(info.setup_command, 'setup')}
              >
                {copied === 'setup' ? 'Copied' : 'Copy'}
              </button>
            </div>
            <div className="step-hint">
              Opens a browser for Databricks OAuth login, tests gateway connectivity, and installs as a background service.
            </div>
          </div>
        </div>

        <div className="setup-step">
          <div className="step-number">2</div>
          <div className="step-content">
            <div className="step-label">Configure {toolConfig?.name ?? tab}</div>
            <div className="step-detail">
              <p>Set environment variables (e.g. in your shell profile or project <code>.env</code>):</p>
              <div className="command-block">
                <code>{envVarSnippet}</code>
                <button
                  className="copy-btn"
                  onClick={() => copyToClipboard(envVarSnippet, 'env')}
                >
                  {copied === 'env' ? 'Copied' : 'Copy'}
                </button>
              </div>
              {tab === 'codex' && (
                <>
                  <p>Or add to <code>~/.codex/config.toml</code>:</p>
                  <div className="command-block">
                    <code>{`[provider]\nbase_url = "http://127.0.0.1:8787/v1"\nenv_key = "OPENAI_API_KEY"\nwire_api = "chat"`}</code>
                    <button
                      className="copy-btn"
                      onClick={() => copyToClipboard(`[provider]\nbase_url = "http://127.0.0.1:8787/v1"\nenv_key = "OPENAI_API_KEY"\nwire_api = "chat"`, 'toml')}
                    >
                      {copied === 'toml' ? 'Copied' : 'Copy'}
                    </button>
                  </div>
                </>
              )}
              {tab === 'opencode' && (
                <>
                  <p>Or add to <code>~/.config/opencode/opencode.jsonc</code>:</p>
                  <div className="command-block">
                    <code>{`{\n  "provider": {\n    "openai-compatible": {\n      "apiKey": "unused",\n      "baseURL": "http://127.0.0.1:8787/v1"\n    }\n  }\n}`}</code>
                    <button
                      className="copy-btn"
                      onClick={() => copyToClipboard(`{\n  "provider": {\n    "openai-compatible": {\n      "apiKey": "unused",\n      "baseURL": "http://127.0.0.1:8787/v1"\n    }\n  }\n}`, 'jsonc')}
                    >
                      {copied === 'jsonc' ? 'Copied' : 'Copy'}
                    </button>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>

        <div className="setup-step">
          <div className="step-number">3</div>
          <div className="step-content">
            <div className="step-label">Verify</div>
            <div className="command-block">
              <code>{info.status_command}</code>
              <button
                className="copy-btn"
                onClick={() => copyToClipboard(info.status_command, 'status')}
              >
                {copied === 'status' ? 'Copied' : 'Copy'}
              </button>
            </div>
            <div className="step-hint">
              Then open a new terminal and run your tool — it should connect through the proxy.
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

function buildUniversalAgentPrompt(info: SetupInfo): string {
  const models = info.model_aliases.length > 0
    ? info.model_aliases.join(', ')
    : 'claude-sonnet-latest, gpt-latest, gemini-latest'

  return `Set up the databricks-agent-proxy so local AI coding tools can use Databricks-hosted LLMs through the Better Agent Gateway.

First, ask the user which tools they want to configure. The supported tools are:
- Claude Code (Anthropic's AI coding CLI)
- Codex (OpenAI's AI coding CLI)
- OpenCode (open-source AI coding tool)

The user may want one or more of these. Proceed with their selection.

## Step 1 — Install and authenticate the proxy

Run this command:

${info.setup_command}

This opens a browser for Databricks OAuth login. The user must complete the login. The proxy then tests gateway connectivity and installs itself as a background service.

## Step 2 — Verify the proxy is running

${info.status_command}

Expected output should show: status ok, authenticated true, gateway_url ${info.gateway_url}

## Step 3 — Configure the selected tools

Apply the configuration below ONLY for the tools the user selected.

### Claude Code

Claude Code uses the Anthropic Messages API through the proxy. Set these environment variables:

export ANTHROPIC_BASE_URL=http://127.0.0.1:8787
export ANTHROPIC_API_KEY=unused

Add these to the user's shell profile (~/.zshrc, ~/.bashrc) or project .env file.

### Codex

Codex uses the OpenAI-compatible API through the proxy. Set these environment variables:

export OPENAI_BASE_URL=http://127.0.0.1:8787/v1
export OPENAI_API_KEY=unused

Or write this to ~/.codex/config.toml:

[provider]
base_url = "http://127.0.0.1:8787/v1"
env_key = "OPENAI_API_KEY"
wire_api = "chat"

### OpenCode

OpenCode uses the OpenAI-compatible API through the proxy. Set these environment variables:

export OPENAI_BASE_URL=http://127.0.0.1:8787/v1
export OPENAI_API_KEY=unused

Or write this to ~/.config/opencode/opencode.jsonc:

{
  "provider": {
    "openai-compatible": {
      "apiKey": "unused",
      "baseURL": "http://127.0.0.1:8787/v1"
    }
  }
}

## Step 4 — Test the connection

curl ${info.health_url}

Expected: {"status": "ok", "authenticated": true, ...}

## Available models

${models}

After setup, the user should open a new terminal and run their tool — it will connect through the proxy automatically.`
}
