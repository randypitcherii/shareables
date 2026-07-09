@echo off
rem get-databricks-token.cmd — mint a short-lived Databricks OAuth access token.
rem
rem SSO/OAuth ONLY. Never emits, accepts, or falls back to a static PAT (dapi...).
rem Delegates to the PowerShell variant so all three helpers share one behavior.
rem
rem Inputs (environment):
rem   DATABRICKS_CONFIG_PROFILE  CLI profile to mint the token with (default: DEFAULT)
rem   DATABRICKS_HOST            optional workspace URL; validated against the profile
rem
rem Output: the access token, alone, on stdout. All diagnostics go to stderr.

where powershell >nul 2>&1
if errorlevel 1 (
    echo error: PowerShell is required by this helper and was not found on PATH. 1>&2
    exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0get-databricks-token.ps1"
exit /b %ERRORLEVEL%
