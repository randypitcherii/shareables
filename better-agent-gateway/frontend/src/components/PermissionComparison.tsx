import { useState } from 'react'

interface ResourceResult {
  count: number
  names: string[]
  error: string | null
}

interface CurrentUser {
  username: string | null
  display_name: string | null
  error: string | null
}

interface ChatResult {
  success: boolean
  model: string
  response: string | null
  error: string | null
}

interface ActorResults {
  current_user: CurrentUser
  catalogs: ResourceResult
  warehouses: ResourceResult
  serving_endpoints: ResourceResult
  chat_completion: ChatResult
}

interface ComparisonData {
  obo_user: ActorResults
  app_sp: ActorResults
}

type ResourceKey = 'catalogs' | 'warehouses' | 'serving_endpoints'

const RESOURCE_LABELS: Record<ResourceKey, string> = {
  catalogs: 'Unity Catalog Catalogs',
  warehouses: 'SQL Warehouses',
  serving_endpoints: 'Serving Endpoints',
}

function IdentityCell({ user }: { user: CurrentUser }) {
  if (user.error) {
    return (
      <div className="perm-cell perm-error">
        <span className="perm-count-badge perm-badge-error">Error</span>
        <p className="perm-error-msg">{user.error}</p>
      </div>
    )
  }
  return (
    <div className="perm-cell">
      <div className="perm-identity-name">{user.display_name || user.username || '—'}</div>
      {user.display_name && user.username && (
        <div className="perm-identity-sub">{user.username}</div>
      )}
    </div>
  )
}

function ResourceCell({ result }: { result: ResourceResult }) {
  if (result.error) {
    return (
      <div className="perm-cell perm-error">
        <span className="perm-count-badge perm-badge-error">Error</span>
        <p className="perm-error-msg">{result.error}</p>
      </div>
    )
  }

  const status = result.count === 0 ? 'perm-badge-none' : result.count <= 3 ? 'perm-badge-limited' : 'perm-badge-ok'

  return (
    <div className="perm-cell">
      <span className={`perm-count-badge ${status}`}>{result.count}</span>
      {result.names.length > 0 && (
        <ul className="perm-name-list">
          {result.names.map((name, i) => (
            <li key={i}>{name}</li>
          ))}
        </ul>
      )}
    </div>
  )
}

function ChatCell({ result }: { result: ChatResult }) {
  if (result.error) {
    return (
      <div className="perm-cell perm-error">
        <span className="perm-count-badge perm-badge-error">Denied</span>
        <p className="perm-error-msg">{result.error}</p>
      </div>
    )
  }

  return (
    <div className="perm-cell">
      <span className="perm-count-badge perm-badge-ok">Success</span>
      {result.response && (
        <p className="perm-chat-response">"{result.response}"</p>
      )}
    </div>
  )
}

export function PermissionComparison() {
  const [data, setData] = useState<ComparisonData | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const runComparison = () => {
    setLoading(true)
    setError(null)
    fetch('/api/v1/permissions/comparison')
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((d) => {
        setData(d)
        setLoading(false)
      })
      .catch((e) => {
        setError(e.message)
        setLoading(false)
      })
  }

  return (
    <div className="perm-comparison">
      <div className="perm-header">
        <h2>Permission Comparison</h2>
        <button
          className="refresh-btn"
          onClick={runComparison}
          disabled={loading}
        >
          {loading ? 'Running...' : data ? 'Re-run Comparison' : 'Run Comparison'}
        </button>
      </div>
      <p className="perm-description">
        Compare what <strong>you</strong> can see (via your OBO token) vs what the
        <strong> app service principal</strong> can see. This demonstrates scoped OAuth
        in action — same workspace, different permissions.
      </p>

      {error && <div className="error">Comparison failed: {error}</div>}

      {loading && !data && (
        <div className="perm-loading">Running API calls as both identities...</div>
      )}

      {data && (
        <div className="perm-grid">
          <div className="perm-grid-header">
            <div className="perm-grid-label"></div>
            <div className="perm-grid-col-header">Your Access (OBO)</div>
            <div className="perm-grid-col-header">App Service Principal</div>
          </div>
          {/* Identity row */}
          <div className="perm-grid-row">
            <div className="perm-grid-label">Identity</div>
            <IdentityCell user={data.obo_user.current_user} />
            <IdentityCell user={data.app_sp.current_user} />
          </div>
          {/* Resource rows */}
          {(Object.keys(RESOURCE_LABELS) as ResourceKey[]).map((key) => (
            <div className="perm-grid-row" key={key}>
              <div className="perm-grid-label">{RESOURCE_LABELS[key]}</div>
              <ResourceCell result={data.obo_user[key]} />
              <ResourceCell result={data.app_sp[key]} />
            </div>
          ))}
          {/* Chat completion check row */}
          <div className="perm-grid-row">
            <div className="perm-grid-label">
              Chat Completion
              <div className="perm-check-model">{data.obo_user.chat_completion.model}</div>
            </div>
            <ChatCell result={data.obo_user.chat_completion} />
            <ChatCell result={data.app_sp.chat_completion} />
          </div>
        </div>
      )}
    </div>
  )
}
