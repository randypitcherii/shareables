import { useState } from 'react'

interface ResourceResult {
  count: number
  names: string[]
  error: string | null
}

interface ComparisonData {
  obo_user: {
    catalogs: ResourceResult
    warehouses: ResourceResult
    serving_endpoints: ResourceResult
  }
  app_sp: {
    catalogs: ResourceResult
    warehouses: ResourceResult
    serving_endpoints: ResourceResult
  }
}

type ResourceKey = 'catalogs' | 'warehouses' | 'serving_endpoints'

const RESOURCE_LABELS: Record<ResourceKey, string> = {
  catalogs: 'Unity Catalog Catalogs',
  warehouses: 'SQL Warehouses',
  serving_endpoints: 'Serving Endpoints',
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
          {(Object.keys(RESOURCE_LABELS) as ResourceKey[]).map((key) => (
            <div className="perm-grid-row" key={key}>
              <div className="perm-grid-label">{RESOURCE_LABELS[key]}</div>
              <ResourceCell result={data.obo_user[key]} />
              <ResourceCell result={data.app_sp[key]} />
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
