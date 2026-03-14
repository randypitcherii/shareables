import { useEffect, useState } from 'react'

interface VersionInfo {
  version: string
  git_hash: string
  git_commit_date: string
}

export function VersionBadge() {
  const [info, setInfo] = useState<VersionInfo | null>(null)

  useEffect(() => {
    fetch('/api/v1/version')
      .then(r => r.json())
      .then(setInfo)
      .catch(() => {})
  }, [])

  if (!info) return null

  const display = [
    `v${info.version}`,
    info.git_hash !== 'unknown' ? info.git_hash : null,
    info.git_commit_date !== 'unknown' ? info.git_commit_date : null,
  ].filter(Boolean).join(' · ')

  return <span className="version-badge">{display}</span>
}
