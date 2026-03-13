import { useEffect, useState } from 'react'

interface CursorConfigResponse {
  instructions: string
  config: {
    apiKey: string
    baseUrl: string
    models: string[]
  }
  notes: string[]
}

interface ValidationCheck {
  name: string
  status: 'pass' | 'fail' | 'info'
  detail: string
}

interface ValidationResponse {
  valid: boolean
  checks: ValidationCheck[]
}

export function CursorConfig() {
  const [data, setData] = useState<CursorConfigResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [copied, setCopied] = useState(false)
  const [validation, setValidation] = useState<ValidationResponse | null>(null)
  const [validating, setValidating] = useState(false)
  const [validationError, setValidationError] = useState<string | null>(null)

  useEffect(() => {
    fetch('/api/v1/config/cursor')
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then(setData)
      .catch((e) => setError(e.message))
  }, [])

  const handleCopy = () => {
    if (!data) return
    const text = JSON.stringify(data.config, null, 2)
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }

  const handleValidate = () => {
    setValidating(true)
    setValidation(null)
    setValidationError(null)
    fetch('/api/v1/config/cursor/validate', { method: 'POST' })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then(setValidation)
      .catch((e) => setValidationError(e.message))
      .finally(() => setValidating(false))
  }

  if (error) return <div className="error">Failed to load Cursor config: {error}</div>
  if (!data) return <div>Loading Cursor config...</div>

  const configJson = JSON.stringify(data.config, null, 2)

  return (
    <div className="cursor-config">
      <h2>Cursor IDE Setup</h2>

      <ol className="cursor-config-steps">
        <li>Open Cursor Settings (<code>Cmd+,</code>)</li>
        <li>Go to <strong>Models</strong> &gt; <strong>OpenAI API Compatible</strong></li>
        <li>Paste the configuration below</li>
        <li>Click <strong>Validate Connection</strong> to verify</li>
      </ol>

      <pre className="cursor-config-code"><code>{configJson}</code></pre>

      <ul className="cursor-config-notes">
        {data.notes.map((note, i) => (
          <li key={i}>{note}</li>
        ))}
      </ul>

      <div className="cursor-config-actions">
        <button
          className={`copy-btn${copied ? ' copied' : ''}`}
          onClick={handleCopy}
        >
          {copied ? 'Copied!' : 'Copy to Clipboard'}
        </button>
        <button
          className="refresh-btn"
          onClick={handleValidate}
          disabled={validating}
        >
          {validating ? 'Validating…' : 'Validate Connection'}
        </button>
      </div>

      {validationError && (
        <div className="error">Validation failed: {validationError}</div>
      )}

      {validation && (
        <ul className="cursor-config-validation">
          {validation.checks.map((check, i) => {
            const icon = check.status === 'pass' ? '✓' : check.status === 'fail' ? '✗' : 'ℹ'
            return (
              <li key={i} className={`cursor-config-check ${check.status}`}>
                <span className="check-icon">{icon}</span>
                <span className="check-name">{check.name}</span>
                {check.detail && (
                  <span className="check-detail">{check.detail}</span>
                )}
              </li>
            )
          })}
        </ul>
      )}
    </div>
  )
}
