# Genie Web Search

A Unity Catalog Python UDF that lets Databricks SQL, Spark, and Genie Code do
live web searches. It is backed by Exa's free public MCP endpoint
(`https://mcp.exa.ai/mcp`) and does not require an API key.

## Files

- `register_web_search.py` registers the UDF. Set `NAMESPACE` before running it.
- `README.md` is this setup guide.

## Requirements

- Unity Catalog enabled workspace.
- `USE CATALOG`, `USE SCHEMA`, and `CREATE FUNCTION` on your target schema.
- Compute with internet egress to `mcp.exa.ai`. Serverless compute works out of the box.

## Usage

1. Pick a `<catalog>.<schema>` where you have `CREATE FUNCTION`.
2. Edit `register_web_search.py` and set `NAMESPACE = "<catalog>.<schema>"`.
3. Paste the file into a Databricks notebook cell on a UC-enabled workspace and run it.
4. Verify the function:

```sql
SELECT <catalog>.<schema>.web_search('latest news on Databricks');
```

5. Wire it up as a Genie Code MCP tool using the manual steps below.

## Configuration

| Setting | Required | Purpose |
| --- | --- | --- |
| `NAMESPACE` | yes | `"<catalog>.<schema>"` where the function is registered. Empty string raises. |
| `NUM_RESULTS` | no | Results per query. Defaults to `10`. |

The function name is hard-coded as `web_search`.

## Picking A Namespace

Use the Databricks CLI to find a sensible target before editing the file:

```bash
databricks current-user me --output JSON
databricks catalogs list --output JSON
databricks schemas list <catalog> --output JSON
databricks grants get-effective schema <catalog>.<schema> --output JSON
```

Heuristics:

1. Look for a personal sandbox such as `users.<sanitized_email>`.
2. Otherwise, look for a schema where you have `ALL_PRIVILEGES` or `CREATE_FUNCTION`.
3. Avoid `main.default` and `hive_metastore.*` unless you explicitly want those.
4. If nothing is obvious, ask the workspace owner instead of guessing.

## Wire It Up As A Genie Code Tool

This step is manual. There is no public API for it.

1. Avatar (top-right) -> Settings -> Genie Code -> MCP Servers -> Add MCP server.
2. Choose Unity Catalog function.
3. Pick the catalog/schema where the function was registered.
4. Toggle `web_search` on and save.
5. Fully reload your Databricks tab. Genie Code only re-reads its tool list on session start.

If an assistant is setting this up, it can register the UDF, but the human must
complete the manual Genie Code MCP wiring.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `SELECT web_search(...)` errors but registration succeeded | No internet egress. Use Serverless or allow outbound HTTPS to `mcp.exa.ai`. |
| `CREATE FUNCTION` denied | Pick a different `NAMESPACE`. |
| `ValueError: NAMESPACE must be...` | Set `NAMESPACE = "<catalog>.<schema>"` at the top of the script. |
| Tool missing in Genie Code after wiring | Fully reload the Databricks tab. |

## How It Works

The UDF speaks JSON-RPC to Exa's hosted public MCP server, calls the
`web_search_exa` tool, and returns the concatenated text blocks from the response.
It uses only the Python standard library so it can run inside the UC Python sandbox
with no extra dependencies.

If Exa changes its public endpoint to require auth, add an `EXA_API_KEY` environment
variable or Databricks secret to the UDF body.

## Engineering Notes

The `CREATE FUNCTION` SQL is built with plain string concatenation, not f-strings
or `.format()`, because the embedded Python source contains `{}` characters that
would clash with format placeholders.

The UDF body is wrapped in a raw triple-quoted string (`r'''...'''`) so backslash
escapes like `\n` survive into the UDF source. Without the `r` prefix, `'\n'.join(...)`
becomes a literal newline in the source and produces an unterminated-string
`SyntaxError` at UDF runtime.

`NUM_RESULTS` is injected into the body via plain concatenation for the same reason.
