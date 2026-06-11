"""
Register a Unity Catalog Python UDF named web_search(query STRING) RETURNS STRING.

The UDF performs a real-time web search via Exa's public MCP endpoint. It is the
"fresh public internet" tool the dispatcher agent routes to.

Adapted from databricks/demos/genie_web_search/register_web_search.py. The only
change is configurability: instead of a hand-edited NAMESPACE constant, the
catalog and schema come from (in priority order):
  1. function arguments to register_web_search(catalog, schema)
  2. notebook widgets `catalog` / `schema` (when run as a notebook cell)
  3. environment variables BASE_CATALOG / BASE_SCHEMA

So this file runs both as a standalone notebook cell AND as a bundle job task
(driver.py imports and calls register_web_search()).
"""

import os

NUM_RESULTS = 10
FUNCTION_NAME = "web_search"


# Authoring conventions (preserved from the original):
# - CREATE FUNCTION SQL is built with plain string concatenation, not f-strings,
#   because the embedded Python source contains `{}` that would clash with
#   format placeholders.
# - udf_body is a raw triple-quoted string so backslash escapes survive into the
#   UDF source (e.g. '\n'.join(...) must stay a backslash-n, not a real newline).
# - NUM_RESULTS is injected with plain concatenation for the same reason.
def _build_create_sql(fqn):
    udf_body = (
        r'''
import json
import urllib.error
import urllib.request

URL = "https://mcp.exa.ai/mcp"
BASE_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}
NUM_RESULTS = '''
        + str(NUM_RESULTS)
        + r'''

def _post(body, headers, timeout=30):
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(URL, data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        text = resp.read().decode("utf-8", errors="replace")
        session = resp.headers.get("Mcp-Session-Id") or resp.headers.get("mcp-session-id")
        return text, session

def _parse(body):
    body = (body or "").strip()
    if not body:
        return None
    if body.startswith("{"):
        try:
            return json.loads(body)
        except Exception:
            return None
    last = None
    for line in body.splitlines():
        line = line.strip()
        if line.startswith("data:"):
            try:
                last = json.loads(line[5:].strip())
            except Exception:
                continue
    return last

def _run(query):
    if query is None or not str(query).strip():
        return json.dumps({"error": "query is empty"})

    try:
        _, session_id = _post(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "databricks-uc-udf", "version": "1.0"},
                },
            },
            BASE_HEADERS,
        )

        headers = dict(BASE_HEADERS)
        if session_id:
            headers["Mcp-Session-Id"] = session_id

        # notifications/initialized returns 202 with no response body.
        try:
            _post(
                {"jsonrpc": "2.0", "method": "notifications/initialized"},
                headers,
                timeout=10,
            )
        except Exception:
            pass

        body, _ = _post(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "web_search_exa",
                    "arguments": {"query": query, "numResults": NUM_RESULTS},
                },
            },
            headers,
            timeout=60,
        )

        parsed = _parse(body)
        if not isinstance(parsed, dict):
            return body or ""

        if "error" in parsed:
            return json.dumps(parsed["error"])

        result = parsed.get("result") or {}
        content = result.get("content") if isinstance(result, dict) else None
        if isinstance(content, list):
            texts = [
                item.get("text", "")
                for item in content
                if isinstance(item, dict) and item.get("type") == "text"
            ]
            if texts:
                return "\n".join(texts)

        return json.dumps(result)
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            err_body = ""
        return json.dumps({"error": "HTTP " + str(e.code), "body": err_body[:500]})
    except Exception as e:
        return json.dumps({"error": str(e)})

return _run(query)
'''
    )

    return (
        "CREATE OR REPLACE FUNCTION " + fqn + "(\n"
        "  query STRING COMMENT 'Natural-language web search query'\n"
        ")\n"
        "RETURNS STRING\n"
        "LANGUAGE PYTHON\n"
        "COMMENT 'Performs a real-time web search and returns the top results as a single text block with Title / URL / Published / Author / Highlights sections separated by ---. Use this whenever you need up-to-date information from the public internet that may not be in your training data.'\n"
        "AS " + ("$" * 2) + udf_body + ("$" * 2) + ";"
    )


def _resolve_namespace(catalog, schema):
    """Resolve (catalog, schema) from args, then widgets, then env vars."""
    if not catalog or not schema:
        # Notebook widgets, if running inside Databricks.
        try:
            catalog = catalog or dbutils.widgets.get("catalog")  # type: ignore[name-defined]  # noqa: F821
            schema = schema or dbutils.widgets.get("schema")  # type: ignore[name-defined]  # noqa: F821
        except Exception:
            pass
    catalog = catalog or os.environ.get("BASE_CATALOG", "")
    schema = schema or os.environ.get("BASE_SCHEMA", "")
    catalog, schema = catalog.strip(), schema.strip()
    if not catalog or not schema:
        raise ValueError(
            "catalog and schema are required. Pass them as arguments, set "
            "notebook widgets `catalog`/`schema`, or set BASE_CATALOG/BASE_SCHEMA. "
            "Got catalog=" + repr(catalog) + ", schema=" + repr(schema)
        )
    return catalog, schema


def register_web_search(catalog="", schema="", spark_session=None):
    """Register {catalog}.{schema}.web_search and return its fully-qualified name.

    Args:
        catalog: UC catalog (optional; falls back to widget/env).
        schema: UC schema (optional; falls back to widget/env).
        spark_session: a SparkSession (optional; uses the ambient `spark` if None).
    """
    catalog, schema = _resolve_namespace(catalog, schema)
    fqn = catalog + "." + schema + "." + FUNCTION_NAME

    active_spark = spark_session
    if active_spark is None:
        # The `spark` global only exists in notebooks. A serverless
        # spark_python_task (how driver.py runs) must build its own session.
        try:
            active_spark = spark  # type: ignore[name-defined]  # noqa: F821
        except NameError:
            from pyspark.sql import SparkSession

            active_spark = SparkSession.builder.getOrCreate()

    # The dev/test schemas are per-user/per-PR and won't pre-exist.
    active_spark.sql("CREATE SCHEMA IF NOT EXISTS " + catalog + "." + schema)
    active_spark.sql(_build_create_sql(fqn))
    print("Registered " + fqn)
    return fqn


# When pasted into a notebook cell, run directly (widgets/env supply the namespace).
if __name__ == "__main__":
    register_web_search()
