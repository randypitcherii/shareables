# get-databricks-token.ps1 — mint a short-lived Databricks OAuth access token.
#
# SSO/OAuth ONLY. This helper never emits, accepts, or falls back to a static
# personal access token (dapi...). If the resolved credential looks like a PAT,
# it refuses.
#
# Inputs (environment):
#   DATABRICKS_CONFIG_PROFILE  CLI profile to mint the token with (default: DEFAULT)
#   DATABRICKS_HOST            optional workspace URL; validated against the profile
#
# Output: the access token, alone, on stdout. All diagnostics go to stderr.

$ErrorActionPreference = "Stop"

$ProfileName = if ($env:DATABRICKS_CONFIG_PROFILE) { $env:DATABRICKS_CONFIG_PROFILE } else { "DEFAULT" }

if (-not (Get-Command databricks -ErrorAction SilentlyContinue)) {
    [Console]::Error.WriteLine("error: 'databricks' CLI not found on PATH.")
    [Console]::Error.WriteLine("Install it (https://docs.databricks.com/dev-tools/cli/install.html), then authenticate with:")
    [Console]::Error.WriteLine("  databricks auth login --host https://<your-workspace> --profile $ProfileName")
    exit 1
}

$cliArgs = @("auth", "token", "--profile", $ProfileName, "-o", "json")
if ($env:DATABRICKS_HOST) {
    $cliArgs += @("--host", $env:DATABRICKS_HOST)
}

$output = & databricks @cliArgs 2>&1
if ($LASTEXITCODE -ne 0) {
    [Console]::Error.WriteLine("error: could not mint an OAuth token for profile '$ProfileName'.")
    [Console]::Error.WriteLine(($output | Out-String).Trim())
    [Console]::Error.WriteLine("Authenticate with: databricks auth login --host https://<your-workspace> --profile $ProfileName")
    exit 1
}

try {
    $token = (($output | Out-String) | ConvertFrom-Json).access_token
} catch {
    $token = $null
}

if (-not $token) {
    [Console]::Error.WriteLine("error: 'databricks auth token' returned no access_token for profile '$ProfileName'.")
    [Console]::Error.WriteLine(($output | Out-String).Trim())
    [Console]::Error.WriteLine("This usually means the profile is not OAuth-authenticated. Run:")
    [Console]::Error.WriteLine("  databricks auth login --host https://<your-workspace> --profile $ProfileName")
    exit 1
}

if ($token.StartsWith("dapi")) {
    [Console]::Error.WriteLine("error: resolved credential looks like a static personal access token (dapi...).")
    [Console]::Error.WriteLine("This experiment is SSO-only — PATs are refused by design. Use OAuth instead:")
    [Console]::Error.WriteLine("  databricks auth login --host https://<your-workspace> --profile $ProfileName")
    exit 1
}

[Console]::Out.WriteLine($token)
