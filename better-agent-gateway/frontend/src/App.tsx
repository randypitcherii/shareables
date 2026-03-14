import { ModelTable } from './components/ModelTable'
import { HealthBadge } from './components/HealthBadge'
import { UserInfo } from './components/UserInfo'
import { PermissionComparison } from './components/PermissionComparison'
import { ProxySetup } from './components/ProxySetup'
import { RequestLogs } from './components/RequestLogs'
import { VersionBadge } from './components/VersionBadge'
import './App.css'

function App() {
  return (
    <div className="app">
      <header>
        <div className="header-left">
          <h1>Better Agent Gateway</h1>
          <VersionBadge />
        </div>
        <HealthBadge />
      </header>
      <main>
        <section className="info-banner">
          <p>
            This gateway proxies LLM requests through Databricks Model Serving
            with <strong>scoped OAuth</strong> — your identity, limited permissions.
            Use <code>*-latest</code> aliases to always hit the newest model version.
          </p>
        </section>
        <ProxySetup />
        <UserInfo />
        <PermissionComparison />
        <ModelTable />
        <RequestLogs />
      </main>
    </div>
  )
}

export default App
