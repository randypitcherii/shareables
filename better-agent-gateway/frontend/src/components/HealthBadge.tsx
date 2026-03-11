import { useEffect, useState } from 'react'

export function HealthBadge() {
  const [healthy, setHealthy] = useState<boolean | null>(null)

  useEffect(() => {
    const check = () =>
      fetch('/api/v1/healthz')
        .then((r) => setHealthy(r.ok))
        .catch(() => setHealthy(false))

    check()
    const interval = setInterval(check, 30_000)
    return () => clearInterval(interval)
  }, [])

  if (healthy === null) return <span className="health-badge loading">Checking...</span>
  return (
    <span className={`health-badge ${healthy ? 'healthy' : 'unhealthy'}`}>
      {healthy ? 'Healthy' : 'Unhealthy'}
    </span>
  )
}
