import { useEffect, useState, useCallback } from 'react'

interface AuthContext {
  forwarded_user: string | null
  obo_token_present: boolean
  mode: string
}

export function UserInfo() {
  const [auth, setAuth] = useState<AuthContext | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [refreshing, setRefreshing] = useState(false)

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

  const handleRefreshConsent = () => {
    setRefreshing(true)
    // Clear browser-side caches that may hold stale OAuth state
    if ('caches' in window) {
      caches.keys().then((names) => names.forEach((name) => caches.delete(name)))
    }
    // Redirect to the app root which triggers a fresh SSO/OAuth consent flow
    // The __clear_session=1 param signals intent (Databricks Apps proxy handles the rest)
    window.location.href = '/?__clear_session=1'
  }

  if (error) return <div className="error">Failed to load user info: {error}</div>
  if (!auth) return <div className="user-info loading">Loading user info...</div>

  const displayUser = auth.forwarded_user
    ? auth.forwarded_user.split('@')[0].replace(/\./g, ' ')
    : 'Unknown'

  const rawUser = auth.forwarded_user || 'Not available'

  return (
    <div className="user-info">
      <h2>Your Session</h2>
      <div className="user-info-grid">
        <div className="user-info-row">
          <span className="label">User</span>
          <span className="value" title={rawUser}>{displayUser}</span>
        </div>
        <div className="user-info-row">
          <span className="label">Identity</span>
          <span className="value mono">{rawUser}</span>
        </div>
        <div className="user-info-row">
          <span className="label">OAuth Token</span>
          <span className={`value badge ${auth.obo_token_present ? 'badge-ok' : 'badge-warn'}`}>
            {auth.obo_token_present ? 'Active' : 'Missing'}
          </span>
        </div>
        <div className="user-info-row">
          <span className="label">Mode</span>
          <span className="value mono">{auth.mode}</span>
        </div>
        <div className="user-info-row">
          <span className="label">Scoped Access</span>
          <span className="value">
            <span className="scope-tag">serving-endpoints</span>
            <span className="scope-tag">sql</span>
          </span>
        </div>
      </div>
      <div className="user-info-actions">
        <button
          className="refresh-btn"
          onClick={handleRefreshConsent}
          disabled={refreshing}
          title="Clear cached OAuth state and re-trigger the consent flow"
        >
          {refreshing ? 'Redirecting...' : 'Refresh OAuth Consent'}
        </button>
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
