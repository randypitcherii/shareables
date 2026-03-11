import { useEffect, useState } from 'react'

interface ModelsResponse {
  aliases: Record<string, string>
  endpoints: string[]
}

export function ModelTable() {
  const [data, setData] = useState<ModelsResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetch('/api/v1/models')
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then(setData)
      .catch((e) => setError(e.message))
  }, [])

  if (error) return <div className="error">Failed to load models: {error}</div>
  if (!data) return <div>Loading models...</div>

  const aliasEntries = Object.entries(data.aliases)

  return (
    <div className="model-table">
      <h2>Model Aliases (-latest)</h2>
      {aliasEntries.length === 0 ? (
        <p>No aliases configured. Serving endpoints may not be available yet.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Alias</th>
              <th>Resolves To</th>
            </tr>
          </thead>
          <tbody>
            {aliasEntries.map(([alias, target]) => (
              <tr key={alias}>
                <td><code>{alias}</code></td>
                <td><code>{target}</code></td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <h2>Available Endpoints ({data.endpoints.length})</h2>
      <ul className="endpoint-list">
        {data.endpoints.map((ep) => (
          <li key={ep}><code>{ep}</code></li>
        ))}
      </ul>
    </div>
  )
}
