# DuckDB uc_catalog Extension: Hostname Resolution Bug

## The Error

```
Could not resolve hostname error for HTTP GET to '/api/2.1/unity-catalog/schemas?catalog_name=fe_randy_pitcher_workspace_catalog'
```

The URL starts with `/` — the host from `ENDPOINT` is not being prepended.

## Root Cause

This is a **named secret lookup bug**, not a URL construction bug.

The source code in `uc_api.cpp` correctly constructs URLs:

```cpp
auto url = credentials.endpoint + "/api/2.1/unity-catalog/schemas?catalog_name=" + catalog.GetDBPath();
```

The `credentials.endpoint` is empty string `""` because the named secret was not found.

In `unity_catalog_extension.cpp`, the `GetSecret()` function only searches two stores:

```cpp
auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "memory");
// ...
secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "local_file");
```

When you use `ATTACH ... (TYPE UC_CATALOG)` **without specifying a SECRET option**, the extension looks for secrets named `__default_unity_catalog` or `__default_uc`. It finds neither (your secret is named `uc_delta`), so `credentials.endpoint` stays as `""`, producing URLs like `/api/2.1/...`.

The result: endpoint is empty, URL becomes `/api/2.1/unity-catalog/schemas?...`, and the HTTP client reports "could not resolve hostname" for a path-only URL.

## Known Issue Status

- **duckdb/unity_catalog issue #48** ("naming secret not supported") — open, reported against v1.3.0, still unresolved as of DuckDB 1.5.0
- The extension is explicitly marked as experimental/proof-of-concept in the README
- No fix has been merged as of March 2026

## Workarounds

### Workaround 1: Use the default secret name (recommended)

Do not name the secret. When no name is given, DuckDB stores it as `__default_uc` (for `TYPE UC`) or `__default_unity_catalog` (for `TYPE UNITY_CATALOG`), which the extension looks up automatically.

```sql
INSTALL uc_catalog;
INSTALL delta;
LOAD delta;
LOAD uc_catalog;

-- No name on the secret — stored as __default_uc
CREATE SECRET (
    TYPE UC,
    TOKEN 'your-databricks-pat',
    ENDPOINT 'https://fe-vm-fe-randy-pitcher-workspace.cloud.databricks.com',
    AWS_REGION 'us-west-2'
);

-- No SECRET option on ATTACH — picks up __default_uc automatically
ATTACH 'fe_randy_pitcher_workspace_catalog' AS uc (TYPE UC_CATALOG);
```

### Workaround 2: Use the SECRET option in ATTACH (if secret name lookup is fixed)

If you need a named secret (e.g., for multiple catalogs with different creds), pass the secret name explicitly in the ATTACH statement. This path goes through `GetSecret(context, secret_name)` which does the "memory"/"local_file" lookup — this should work for in-session secrets if the lookup is matching correctly.

```sql
CREATE SECRET my_uc_secret (
    TYPE UC,
    TOKEN 'your-databricks-pat',
    ENDPOINT 'https://fe-vm-fe-randy-pitcher-workspace.cloud.databricks.com',
    AWS_REGION 'us-west-2'
);

ATTACH 'fe_randy_pitcher_workspace_catalog' AS uc (TYPE UC_CATALOG, SECRET my_uc_secret);
```

> Note: Issue #48 reports this does NOT work in v1.3.0. Whether it works in 1.5.0 is unverified — the code path exists but `GetSecretByName` may not match in-session secrets by name reliably. Try Workaround 1 first.

### Workaround 3: Specify DEFAULT_SCHEMA to avoid the `/api/2.0/settings/...` pre-flight call

Even with the correct secret, the extension attempts to auto-detect the default schema via a Databricks-specific endpoint (`/api/2.0/settings/types/default_namespace_ws/names/default`) which is not available in OSS Unity Catalog. This failure is caught and logged silently, but the empty default schema can cause issues. Specify it explicitly:

```sql
ATTACH 'fe_randy_pitcher_workspace_catalog' AS uc (
    TYPE UC_CATALOG,
    DEFAULT_SCHEMA 'your_schema_name'
);
```

## Summary

| Approach | Status |
|---|---|
| Named secret + no SECRET in ATTACH | Broken — endpoint is empty, URL malformed |
| Unnamed secret + no SECRET in ATTACH | Works (secret found as `__default_uc`) |
| Named secret + SECRET option in ATTACH | Possibly broken (issue #48, unresolved) |
| Environment variable override | Not supported — no `DUCKDB_UNITY_CATALOG_ENDPOINT` env var exists |

## References

- Extension repo: https://github.com/duckdb/unity_catalog
- Issue #48 (naming secret not supported): https://github.com/duckdb/unity_catalog/issues/48
- Issue #43 (setup local UC connection): https://github.com/duckdb/unity_catalog/issues/43
- Source — secret lookup: `src/unity_catalog_extension.cpp` (`GetSecret`, `UnityCatalogAttach`)
- Source — URL construction: `src/uc_api.cpp` (`GetSchemas`, `GetTables`)
