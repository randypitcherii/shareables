import { useEffect, useState } from 'react'

interface ToolConfig {
  name: string
  description: string
  config_file: string
  config_content: Record<string, unknown>
  config_hint: string
}

interface SetupInfo {
  gateway_url: string
  uvx_from_spec: string
  setup_command: string
  start_command: string
  status_command: string
  health_url: string
  cursor_base_url: string
  proxy_base_url: string
  version: string
  git_hash: string
  model_aliases: string[]
  tool_configs: Record<string, ToolConfig>
}

function ToolIcon({ tool }: { tool: string }) {
  if (tool === 'claude-code') {
    return <img src="https://cdn.jsdelivr.net/npm/simple-icons@latest/icons/claude.svg" alt="" className="tab-icon" />
  }
  if (tool === 'codex') {
    return <img src="https://cdn.jsdelivr.net/npm/simple-icons@latest/icons/openai.svg" alt="" className="tab-icon" />
  }
  return <span className="tab-icon tab-icon-text">{'\u2334'}</span>
}

type ToolTab = 'claude-code' | 'codex' | 'crush'

const TAB_CONFIG_KEYS: Record<ToolTab, string> = {
  'claude-code': 'claude_code',
  'codex': 'codex',
  'crush': 'crush',
}

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
        {(['claude-code', 'codex', 'crush'] as ToolTab[]).map(t => {
          const configKey = TAB_CONFIG_KEYS[t]
          const name = info.tool_configs?.[configKey]?.name ?? t
          return (
            <button
              key={t}
              className={`setup-tab ${tab === t ? 'active' : ''}`}
              onClick={() => setTab(t)}
            >
              <ToolIcon tool={t} /> {name}
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
  const configKey = TAB_CONFIG_KEYS[tab]
  const toolConfig = info.tool_configs?.[configKey]

  const configJson = toolConfig?.config_content
    ? JSON.stringify(toolConfig.config_content, null, 2)
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
              <p>
                Write to <code>{toolConfig?.config_file}</code>:
              </p>
              <div className="command-block">
                <code>{configJson}</code>
                <button
                  className="copy-btn"
                  onClick={() => copyToClipboard(configJson, 'config')}
                >
                  {copied === 'config' ? 'Copied' : 'Copy'}
                </button>
              </div>
              {toolConfig?.config_hint && (
                <div className="step-hint">{toolConfig.config_hint}</div>
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

  const proxyBase = info.proxy_base_url || 'http://127.0.0.1:8787'

  const claudeConfig = info.tool_configs?.claude_code
  const codexConfig = info.tool_configs?.codex
  const crushConfig = info.tool_configs?.crush

  return `Set up the databricks-agent-proxy so local AI coding tools can use Databricks-hosted LLMs through the Better Agent Gateway.

First, ask the user which tools they want to configure. The supported tools are:
- Claude Code (Anthropic's AI coding CLI)
- Codex (OpenAI's AI coding CLI)
- Crush (Charmbracelet's AI coding CLI, formerly OpenCode)

The user may want one or more of these. Proceed with their selection.

## Step 1 — Install and authenticate the proxy

Run this command:

${info.setup_command}

This opens a browser for Databricks OAuth login. The user must complete the login. The proxy then tests gateway connectivity and installs itself as a background service.

## Step 2 — Verify the proxy is running

${info.status_command}

Expected output should show: status ok, authenticated true, gateway_url ${info.gateway_url}

## Step 3 — Configure the selected tools

IMPORTANT: Use settings files, NOT global environment variables. Global env vars like OPENAI_BASE_URL or ANTHROPIC_BASE_URL will break other tools. The configs below are scoped to each tool only.

Apply the configuration below ONLY for the tools the user selected.

### Claude Code

Write to \`${claudeConfig?.config_file || '.claude/settings.local.json'}\`:

${claudeConfig ? JSON.stringify(claudeConfig.config_content, null, 2) : `{
  "env": {
    "ANTHROPIC_BASE_URL": "${proxyBase}",
    "ANTHROPIC_API_KEY": "unused"
  }
}`}

This is scoped to Claude Code only — it does not affect other tools or global env vars. The file is gitignored by default.

### Codex

Write to \`${codexConfig?.config_file || '~/.codex/config.json'}\`:

${codexConfig ? JSON.stringify(codexConfig.config_content, null, 2) : `{
  "model": "claude-sonnet-latest",
  "provider": "databricks-proxy",
  "providers": {
    "databricks-proxy": {
      "name": "Databricks Proxy",
      "baseURL": "${proxyBase}/v1",
      "envKey": "CODEX_PROXY_KEY"
    }
  }
}`}

Also create a .env file in the project root (or export in shell) with:
CODEX_PROXY_KEY=unused

Uses a named provider — does not override OPENAI_BASE_URL.

### Crush (formerly OpenCode)

Write to \`${crushConfig?.config_file || '.crush.json'}\` in the project root:

${crushConfig ? JSON.stringify(crushConfig.config_content, null, 2) : `{
  "providers": {
    "databricks-proxy": {
      "type": "openai-compat",
      "base_url": "${proxyBase}/v1",
      "api_key": "unused"
    }
  }
}`}

Project-level config — does not affect global settings.

## Step 4 — Test the connection

curl ${info.health_url}

Expected: {"status": "ok", "authenticated": true, ...}

## Available models

${models}

After setup, the user should open a new terminal and run their tool — it will connect through the proxy automatically.`
}
