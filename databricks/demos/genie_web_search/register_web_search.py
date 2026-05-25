"""
Register a Unity Catalog Python UDF named web_search(query STRING) RETURNS STRING.

The UDF performs a real-time web search via Exa's public MCP endpoint. Run this
script in a Databricks notebook cell on a UC-enabled workspace after setting
NAMESPACE below.
"""

# Required: "<catalog>.<schema>" where you have CREATE FUNCTION privileges.
# See README.md for namespace selection, Genie Code wiring, and troubleshooting.
NAMESPACE = ""
NUM_RESULTS = 10

FUNCTION_NAME = "web_search"

_parts = NAMESPACE.split(".")
if len(_parts) != 2 or not all(part.strip() for part in _parts):
    raise ValueError(
        "NAMESPACE must be '<catalog>.<schema>' (e.g. 'users.alice'). Got: "
        + repr(NAMESPACE)
    )
FQN = NAMESPACE + "." + FUNCTION_NAME

# Authoring conventions:
# - CREATE FUNCTION SQL is built with plain string concatenation, not f-strings.
# - udf_body is a raw triple-quoted string so backslash escapes survive.
# - NUM_RESULTS is injected with plain concatenation.
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

create_sql = (
    "CREATE OR REPLACE FUNCTION " + FQN + "(\n"
    "  query STRING COMMENT 'Natural-language web search query'\n"
    ")\n"
    "RETURNS STRING\n"
    "LANGUAGE PYTHON\n"
    "COMMENT 'Performs a real-time web search and returns the top results as a single text block with Title / URL / Published / Author / Highlights sections separated by ---. Use this whenever you need up-to-date information from the public internet that may not be in your training data.'\n"
    "AS " + ("$" * 2) + udf_body + ("$" * 2) + ";"
)

spark.sql(create_sql)
print("Registered " + FQN)
