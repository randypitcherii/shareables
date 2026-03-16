import { useEffect, useState, useCallback } from 'react'

interface AuthContext {
  forwarded_user: string | null
  display_name: string | null
  email: string | null
  user_name: string | null
  obo_token_present: boolean
  auth_method: string
  identity_source: string
  scoped_permissions: string[]
}

function formatForwardedUser(forwarded: string | null): string {
  if (!forwarded) return 'Unknown'
  return forwarded.split('@')[0].replace(/\./g, ' ')
}

export function UserInfo() {
  const [auth, setAuth] = useState<AuthContext | null>(null)
  const [error, setError] = useState<string | null>(null)

  const fetchAuth = useCallback(() => {
    fetch('/api/v1/auth/context')
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then(setAuth)
      .catch((e) => setError(e.message))
  }, [])

  useEffect(() => {
    fetchAuth()
  }, [fetchAuth])

  if (error) return <div className="error">Failed to load user info: {error}</div>
  if (!auth) return <div className="user-info loading">Loading user info...</div>

  const displayUser = auth.display_name
    || auth.email
    || formatForwardedUser(auth.forwarded_user)

  const identityValue = auth.email || auth.user_name || auth.forwarded_user || 'Not available'

  return (
    <div className="user-info">
      <h2>Your Session</h2>
      <div className="user-info-grid">
        <div className="user-info-row">
          <span className="label">User</span>
          <span className="value" title={identityValue}>{displayUser}</span>
        </div>
        <div className="user-info-row">
          <span className="label">Identity</span>
          <span className="value mono">{identityValue}</span>
        </div>
        <div className="user-info-row">
          <span className="label">Auth</span>
          <span className={`value badge ${auth.obo_token_present ? 'badge-ok' : 'badge-warn'}`}>
            {auth.obo_token_present ? 'OBO Token (your identity)' : 'No token'}
          </span>
        </div>
        <div className="user-info-row">
          <span className="label">How it works</span>
          <span className="value hint">{auth.identity_source}</span>
        </div>
        <div className="user-info-row">
          <span className="label">Scoped Access</span>
          <span className="value">
            {(auth.scoped_permissions || []).map((scope) => (
              <span key={scope} className="scope-tag">{scope}</span>
            ))}
          </span>
        </div>
      </div>
      <div className="user-info-actions">
        <button
          className="refresh-btn secondary"
          onClick={fetchAuth}
          title="Re-fetch auth context from the server"
        >
          Refresh Status
        </button>
      </div>
    </div>
  )
}
