import { ModelTable } from './components/ModelTable'
import { HealthBadge } from './components/HealthBadge'
import './App.css'

function App() {
  return (
    <div className="app">
      <header>
        <h1>Better Agent Gateway</h1>
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
        <ModelTable />
      </main>
    </div>
  )
}

export default App
