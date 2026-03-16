# databricks-agent-proxy

Localhost OAuth proxy for connecting Cursor IDE (and other OpenAI-compatible clients) to Databricks Apps gateways.

## Quick Start

```bash
uvx databricks-agent-proxy setup --gateway-url https://your-gateway.databricks.app
```

## Usage

```bash
# One-time setup: authenticate + install service + print Cursor config
databricks-agent-proxy setup --gateway-url https://your-gateway.databricks.app

# Manual foreground start
databricks-agent-proxy start --gateway-url https://your-gateway.databricks.app

# Check health
databricks-agent-proxy status

# Remove
databricks-agent-proxy uninstall
```
