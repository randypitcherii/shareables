import { useEffect, useState, useCallback } from 'react'
import Plot from 'react-plotly.js'

interface LogEntry {
  id: string
  timestamp: string
  user_id: string
  model_requested: string
  model_resolved: string
  latency_ms: number
  status_code: number
  prompt_tokens: number | null
  completion_tokens: number | null
  total_tokens: number | null
}

interface LogsResponse {
  logs: LogEntry[]
  total: number
}

interface SummaryEntry {
  model: string
  count: number
  avg_latency_ms: number
  total_tokens: number
}

interface SummaryResponse {
  summary: SummaryEntry[]
  total_requests: number
}

export function RequestLogs() {
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [summary, setSummary] = useState<SummaryEntry[]>([])
  const [totalRequests, setTotalRequests] = useState(0)
  const [error, setError] = useState<string | null>(null)

  const fetchData = useCallback(() => {
    Promise.all([
      fetch('/api/v1/logs?limit=200').then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json() as Promise<LogsResponse>
      }),
      fetch('/api/v1/logs/summary').then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json() as Promise<SummaryResponse>
      }),
    ])
      .then(([logsData, summaryData]) => {
        setLogs(logsData.logs)
        setSummary(summaryData.summary)
        setTotalRequests(summaryData.total_requests)
        setError(null)
      })
      .catch(e => setError(e.message))
  }, [])

  useEffect(() => {
    fetchData()
    const interval = setInterval(fetchData, 10_000)
    return () => clearInterval(interval)
  }, [fetchData])

  if (error) return <div className="error">Failed to load request logs: {error}</div>

  // Build time-series data grouped by model
  const timeSeriesByModel: Record<string, { x: string[]; y: number[] }> = {}
  for (const log of [...logs].reverse()) {
    const model = log.model_resolved
    const minute = log.timestamp.slice(0, 16) // group by minute
    if (!timeSeriesByModel[model]) {
      timeSeriesByModel[model] = { x: [], y: [] }
    }
    const series = timeSeriesByModel[model]
    if (series.x[series.x.length - 1] === minute) {
      series.y[series.y.length - 1] += 1
    } else {
      series.x.push(minute)
      series.y.push(1)
    }
  }

  const plotTraces = Object.entries(timeSeriesByModel).map(([model, data]) => ({
    x: data.x,
    y: data.y,
    type: 'bar' as const,
    name: model,
  }))

  return (
    <div className="request-logs">
      <h2>Request Logs ({totalRequests} total)</h2>

      {summary.length > 0 && (
        <div className="logs-summary">
          <h3>Summary by Model</h3>
          <table>
            <thead>
              <tr>
                <th>Model</th>
                <th>Requests</th>
                <th>Avg Latency</th>
                <th>Total Tokens</th>
              </tr>
            </thead>
            <tbody>
              {summary.map(s => (
                <tr key={s.model}>
                  <td><code>{s.model}</code></td>
                  <td>{s.count}</td>
                  <td>{s.avg_latency_ms}ms</td>
                  <td>{s.total_tokens.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {plotTraces.length > 0 && (
        <div className="logs-chart">
          <Plot
            data={plotTraces}
            layout={{
              title: { text: 'Requests Over Time by Model' },
              barmode: 'stack',
              xaxis: { title: { text: 'Time (UTC)' } },
              yaxis: { title: { text: 'Requests' } },
              margin: { t: 40, r: 20, b: 60, l: 50 },
              height: 300,
              paper_bgcolor: 'transparent',
              plot_bgcolor: 'transparent',
              font: { color: '#e0e0e0' },
            }}
            config={{ responsive: true, displayModeBar: false }}
            style={{ width: '100%' }}
          />
        </div>
      )}

      {logs.length === 0 ? (
        <p>No requests logged yet. Send a chat completion to see logs here.</p>
      ) : (
        <div className="logs-table-wrapper">
          <h3>Recent Requests</h3>
          <table>
            <thead>
              <tr>
                <th>Time (UTC)</th>
                <th>User</th>
                <th>Requested</th>
                <th>Resolved</th>
                <th>Latency</th>
                <th>Status</th>
                <th>Tokens</th>
              </tr>
            </thead>
            <tbody>
              {logs.slice(0, 50).map(log => (
                <tr key={log.id} className={log.status_code !== 200 ? 'error-row' : ''}>
                  <td>{new Date(log.timestamp).toLocaleTimeString()}</td>
                  <td>{log.user_id}</td>
                  <td><code>{log.model_requested}</code></td>
                  <td><code>{log.model_resolved}</code></td>
                  <td>{log.latency_ms}ms</td>
                  <td className={log.status_code === 200 ? 'status-ok' : 'status-error'}>
                    {log.status_code}
                  </td>
                  <td>{log.total_tokens ?? '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
