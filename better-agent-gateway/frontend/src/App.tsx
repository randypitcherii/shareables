import { useState } from 'react'
import { ModelTable } from './components/ModelTable'
import { HealthBadge } from './components/HealthBadge'
import { UserInfo } from './components/UserInfo'
import { PermissionComparison } from './components/PermissionComparison'
import { ProxySetup } from './components/ProxySetup'
import { RequestLogs } from './components/RequestLogs'
import { VersionBadge } from './components/VersionBadge'
import './App.css'

type PageTab = 'setup' | 'usage' | 'models'

function App() {
  const [activeTab, setActiveTab] = useState<PageTab>('setup')

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
        <div className="page-tabs">
          <button
            className={`page-tab${activeTab === 'setup' ? ' active' : ''}`}
            onClick={() => setActiveTab('setup')}
          >
            Setup
          </button>
          <button
            className={`page-tab${activeTab === 'usage' ? ' active' : ''}`}
            onClick={() => setActiveTab('usage')}
          >
            Usage
          </button>
          <button
            className={`page-tab${activeTab === 'models' ? ' active' : ''}`}
            onClick={() => setActiveTab('models')}
          >
            Models
          </button>
        </div>
        {activeTab === 'setup' && (
          <div className="page-tab-content">
            <ProxySetup />
            <UserInfo />
          </div>
        )}
        {activeTab === 'usage' && (
          <div className="page-tab-content">
            <RequestLogs />
            <PermissionComparison />
          </div>
        )}
        {activeTab === 'models' && (
          <div className="page-tab-content">
            <ModelTable />
          </div>
        )}
      </main>
    </div>
  )
}

export default App
