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

type Tab = 'claude-code' | 'codex' | 'opencode' | 'agent'

export function ProxySetup() {
  const [info, setInfo] = useState<SetupInfo | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [copied, setCopied] = useState<string | null>(null)
  const [tab, setTab] = useState<Tab>('claude-code')

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

  const agentPrompt = buildAgentPrompt(info)

  return (
    <section className="proxy-setup">
      <h2>Connect Your AI Coding Tool</h2>
      <p className="proxy-description">
        Use the <strong>databricks-agent-proxy</strong> to connect your AI coding tool
        to this gateway. The proxy runs on localhost, handles OAuth automatically, and forwards requests here.
      </p>

      <div className="setup-tabs">
        <button
          className={`setup-tab ${tab === 'claude-code' ? 'active' : ''}`}
          onClick={() => setTab('claude-code')}
        >
          Claude Code
        </button>
        <button
          className={`setup-tab ${tab === 'codex' ? 'active' : ''}`}
          onClick={() => setTab('codex')}
        >
          Codex
        </button>
        <button
          className={`setup-tab ${tab === 'opencode' ? 'active' : ''}`}
          onClick={() => setTab('opencode')}
        >
          OpenCode
        </button>
        <button
          className={`setup-tab ${tab === 'agent' ? 'active' : ''}`}
          onClick={() => setTab('agent')}
        >
          Agent Prompt
        </button>
      </div>

      {tab === 'agent' ? (
        <div className="setup-tab-content">
          <p className="tab-hint">
            Copy this prompt and paste it into any devtools agent to automate setup.
          </p>
          <div className="command-block agent-block">
            <pre>{agentPrompt}</pre>
            <button
              className="copy-btn"
              onClick={() => copyToClipboard(agentPrompt, 'agent')}
            >
              {copied === 'agent' ? 'Copied' : 'Copy'}
            </button>
          </div>
        </div>
      ) : (
        <ToolSetupTab
          tab={tab}
          info={info}
          copied={copied}
          copyToClipboard={copyToClipboard}
        />
      )}
    </section>
  )
}

function ToolSetupTab({
  tab,
  info,
  copied,
  copyToClipboard,
}: {
  tab: 'claude-code' | 'codex' | 'opencode'
  info: SetupInfo
  copied: string | null
  copyToClipboard: (text: string, label: string) => void
}) {
  const configKey = tab === 'claude-code' ? 'claude_code' : tab
  const toolConfig = info.tool_configs?.[configKey]
  const toolDescription = toolConfig?.description ?? ''

  const envVarSnippet = toolConfig?.env_vars
    ? Object.entries(toolConfig.env_vars)
        .map(([k, v]) => `export ${k}=${v}`)
        .join('\n')
    : ''

  return (
    <div className="setup-tab-content">
      <p className="tab-hint">{toolDescription}. Uses the local proxy for OAuth-authenticated requests.</p>
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

      <div className="agent-prompt-cta">
        <button
          className="agent-prompt-btn"
          onClick={() => copyToClipboard(buildToolAgentPrompt(tab, info), 'tool-agent')}
        >
          {copied === 'tool-agent' ? 'Copied!' : 'Copy instructions as agent prompt'}
        </button>
        <span className="agent-prompt-hint">Paste into any AI coding agent to automate all setup steps</span>
      </div>
    </div>
  )
}

function buildToolAgentPrompt(tab: 'claude-code' | 'codex' | 'opencode', info: SetupInfo): string {
  const models = info.model_aliases.length > 0
    ? info.model_aliases.join(', ')
    : 'claude-sonnet-latest, gpt-latest, gemini-latest'

  const configKey = tab === 'claude-code' ? 'claude_code' : tab
  const toolConfig = info.tool_configs?.[configKey]
  const toolName = toolConfig?.name ?? tab

  const envVars = toolConfig?.env_vars
    ? Object.entries(toolConfig.env_vars)
        .map(([k, v]) => `export ${k}=${v}`)
        .join('\n')
    : ''

  let configSection = `## Configure ${toolName}

Set these environment variables in your shell profile or project .env:

${envVars}`

  if (tab === 'codex') {
    configSection += `

Alternatively, write this to ~/.codex/config.toml:

[provider]
base_url = "http://127.0.0.1:8787/v1"
env_key = "OPENAI_API_KEY"
wire_api = "chat"`
  }

  if (tab === 'opencode') {
    configSection += `

Alternatively, write this to ~/.config/opencode/opencode.jsonc:

{
  "provider": {
    "openai-compatible": {
      "apiKey": "unused",
      "baseURL": "http://127.0.0.1:8787/v1"
    }
  }
}`
  }

  return `Set up the databricks-agent-proxy so ${toolName} can use Databricks-hosted LLMs through the Better Agent Gateway.

## Step 1 — Install and authenticate the proxy

${info.setup_command}

This will open a browser for Databricks OAuth login. Complete the login, then the proxy will test gateway connectivity and install itself as a background service.

## Step 2 — Verify the proxy is running

${info.status_command}

Expected: status ok, authenticated true, gateway_url ${info.gateway_url}

## Step 3 — Configure ${toolName}

${configSection}

## Step 4 — Test the connection

curl ${info.health_url}

Expected: {"status": "ok", "authenticated": true, ...}

## Available models

${models}

After setup, open a new terminal and run ${toolName} — it should connect through the proxy automatically.`
}

function buildAgentPrompt(info: SetupInfo): string {
  const models = info.model_aliases.length > 0
    ? info.model_aliases.join(', ')
    : 'claude-sonnet-latest, gpt-latest, gemini-latest'

  return `Set up the databricks-agent-proxy so AI coding tools (Claude Code, Codex, OpenCode) can use Databricks-hosted LLMs through the Better Agent Gateway.

## Install and configure

${info.setup_command}

This will open a browser for Databricks OAuth login. Complete the login, then the proxy will test gateway connectivity and install itself as a background service.

## Verify

${info.status_command}

Expected: status ok, authenticated true, gateway_url ${info.gateway_url}

## Tool Configuration

### Claude Code (Anthropic Messages API)
export ANTHROPIC_BASE_URL=http://127.0.0.1:8787
export ANTHROPIC_API_KEY=unused

### Codex (OpenAI-compatible)
export OPENAI_BASE_URL=http://127.0.0.1:8787/v1
export OPENAI_API_KEY=unused

### OpenCode (OpenAI-compatible)
export OPENAI_BASE_URL=http://127.0.0.1:8787/v1
export OPENAI_API_KEY=unused

## Available models
${models}

## Test

curl ${info.health_url}`
}
