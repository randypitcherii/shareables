import { useEffect, useState } from 'react'

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
}

type Tab = 'agent' | 'human'

export function ProxySetup() {
  const [info, setInfo] = useState<SetupInfo | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [copied, setCopied] = useState<string | null>(null)
  const [tab, setTab] = useState<Tab>('agent')

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
      <h2>Connect Cursor IDE</h2>
      <p className="proxy-description">
        Use the <strong>databricks-agent-proxy</strong> to connect Cursor (or any OpenAI-compatible client)
        to this gateway. The proxy runs on localhost, handles OAuth automatically, and forwards requests here.
      </p>

      <div className="setup-tabs">
        <button
          className={`setup-tab ${tab === 'agent' ? 'active' : ''}`}
          onClick={() => setTab('agent')}
        >
          Agent Instructions
        </button>
        <button
          className={`setup-tab ${tab === 'human' ? 'active' : ''}`}
          onClick={() => setTab('human')}
        >
          Manual Steps
        </button>
      </div>

      {tab === 'agent' ? (
        <div className="setup-tab-content">
          <p className="tab-hint">
            Copy this prompt and paste it into Claude Code, Cursor Agent, or any devtools agent.
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
        <div className="setup-tab-content">
          <div className="setup-steps">
            <div className="setup-step">
              <div className="step-number">1</div>
              <div className="step-content">
                <div className="step-label">Run setup (authenticates + installs service)</div>
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
                  Installs the proxy via uvx, opens a browser for Databricks OAuth login, tests gateway connectivity, and installs as a background service.
                </div>
              </div>
            </div>

            <div className="setup-step">
              <div className="step-number">2</div>
              <div className="step-content">
                <div className="step-label">Configure Cursor</div>
                <div className="step-detail">
                  <ol>
                    <li>Open <strong>Cursor Settings &rarr; Models</strong></li>
                    <li>Set <em>Override OpenAI Base URL</em> to:</li>
                  </ol>
                  <div className="command-block">
                    <code>{info.cursor_base_url}</code>
                    <button
                      className="copy-btn"
                      onClick={() => copyToClipboard(info.cursor_base_url, 'url')}
                    >
                      {copied === 'url' ? 'Copied' : 'Copy'}
                    </button>
                  </div>
                  <ol start={3}>
                    <li>Add models from the table below as custom models</li>
                    <li>Enter any string as the API key (Cursor requires one, but the proxy ignores it)</li>
                  </ol>
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
              </div>
            </div>
          </div>
        </div>
      )}
    </section>
  )
}

function buildAgentPrompt(info: SetupInfo): string {
  const models = info.model_aliases.length > 0
    ? info.model_aliases.join(', ')
    : 'claude-sonnet-latest, gpt-latest, gemini-latest'

  return `Set up the databricks-agent-proxy so Cursor IDE can use Databricks-hosted LLMs through the Better Agent Gateway.

## Install and configure

${info.setup_command}

This will open a browser for Databricks OAuth login. Complete the login, then the proxy will test gateway connectivity and install itself as a background service.

## Verify

${info.status_command}

Expected: status ok, authenticated true, gateway_url ${info.gateway_url}

## Cursor Settings

- Override OpenAI Base URL: ${info.cursor_base_url}
- API Key: any non-empty string (proxy ignores it)
- Add these as custom models: ${models}

## Test

curl ${info.health_url}`
}
