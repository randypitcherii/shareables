#!/usr/bin/env bash
# setup_helper.sh — mechanical companion for the unity-ai-gateway-coding-agent-setup skill.
#
# Subcommands:
#   preflight
#       Checks: databricks CLI, OAuth auth state (PATs refused), ucode
#       availability, package-registry reachability. Prints a status table.
#       Exits non-zero if a blocking check (CLI / OAuth) fails.
#
#   materialize <agent> <workspace-host> <model> [--profile <p>] [--out <path>]
#                                                [--allow-legacy --reason "<why>"]
#       Renders the matching ../../configs/ template with placeholders
#       substituted, to stdout or --out. <model> must be a three-part Unity
#       Catalog id (catalog.schema.model); legacy single-string names are
#       refused unless --allow-legacy is passed WITH a mandatory --reason,
#       which is embedded in the output as an annotation.
#
#   verify
#       Delegates to `make -C <experiment-root> verify` (one real SSO-OAuth
#       chat completion; HTTP 200 required).
#
# SSO/OAuth only — this script never creates, accepts, or embeds a PAT.

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
EXPERIMENT_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)
CONFIGS_DIR="$EXPERIMENT_ROOT/configs"

usage() {
    cat <<'EOF'
setup_helper.sh — mechanical companion for the unity-ai-gateway-coding-agent-setup skill.

Subcommands:
  preflight
      Checks: databricks CLI, OAuth auth state (PATs refused), ucode
      availability, package-registry reachability. Prints a status table.
      Exits non-zero if a blocking check (CLI / OAuth) fails.

  materialize <agent> <workspace-host> <model> [--profile <p>] [--out <path>]
                                               [--allow-legacy --reason "<why>"]
      Renders the matching configs/ template with placeholders substituted,
      to stdout or --out. <agent> is one of: claude-code | claude-desktop |
      codex | opencode. <model> must be a three-part Unity Catalog id
      (catalog.schema.model); legacy single-string names are refused unless
      --allow-legacy is passed WITH a mandatory --reason, which is embedded
      in the output as an annotation.

  verify
      Delegates to `make -C <experiment-root> verify` (one real SSO-OAuth
      chat completion; HTTP 200 required).

SSO/OAuth only — this script never creates, accepts, or embeds a PAT.
EOF
}

die() {
    echo "error: $*" >&2
    exit 2
}

# ---------------------------------------------------------------- preflight

row() { # row <check> <status> <detail>
    printf '%-28s %-8s %s\n' "$1" "$2" "$3"
}

probe_url() { # probe_url <url> -> 0 reachable / non-zero not
    # Network-level reachability only: any HTTP response (even 401/404 from an
    # auth-required index) counts as reachable; DNS/connect/timeout do not.
    curl -sSL --max-time 5 -o /dev/null "$1" 2>/dev/null
}

cmd_preflight() {
    local profile="${DATABRICKS_CONFIG_PROFILE:-DEFAULT}"
    local blocking_failure=0

    printf '%-28s %-8s %s\n' "CHECK" "STATUS" "DETAIL"
    printf '%-28s %-8s %s\n' "-----" "------" "------"

    # --- databricks CLI ---
    local have_cli=0
    if command -v databricks >/dev/null 2>&1; then
        have_cli=1
        local cli_version
        cli_version=$(databricks --version 2>/dev/null | head -n 1 || true)
        row "databricks CLI" "OK" "${cli_version:-found} ($(command -v databricks))"
    else
        row "databricks CLI" "FAIL" "not on PATH — install the Databricks CLI, then re-run"
        blocking_failure=1
    fi

    # --- OAuth auth state ---
    if [ "$have_cli" -eq 1 ]; then
        local describe_out auth_type auth_user auth_host
        if describe_out=$(databricks auth describe -o json 2>/dev/null); then
            auth_type=$(printf '%s\n' "$describe_out" | sed -n 's/.*"auth_type"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)
            auth_user=$(printf '%s\n' "$describe_out" | sed -n 's/.*"username"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)
            auth_host=$(printf '%s\n' "$describe_out" | sed -n 's/.*"host"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)
            if [ "$auth_type" = "pat" ]; then
                row "OAuth (profile $profile)" "FAIL" "auth_type=pat — PATs are refused; run: databricks auth login --host <workspace-host>"
                blocking_failure=1
            else
                row "OAuth (profile $profile)" "OK" "auth_type=${auth_type:-unknown} user=${auth_user:-?} host=${auth_host:-?}"
            fi
        else
            row "OAuth (profile $profile)" "FAIL" "not authenticated — run: databricks auth login --host <workspace-host> (human completes browser SSO)"
            blocking_failure=1
        fi
    else
        row "OAuth (profile $profile)" "SKIP" "requires the databricks CLI"
    fi

    # --- ucode ---
    if command -v ucode >/dev/null 2>&1; then
        row "ucode" "OK" "happy path: ucode <agent> (claude / codex / opencode)"
    else
        row "ucode" "MISSING" "fall back to escape-hatch templates in configs/ (see SKILL.md Step 2)"
    fi

    # --- package registries (generic; honors any already-configured index) ---
    local pypi_url npm_url
    pypi_url="${UV_INDEX_URL:-${PIP_INDEX_URL:-https://pypi.org/simple/}}"
    npm_url=""
    if command -v npm >/dev/null 2>&1; then
        npm_url=$(npm config get registry 2>/dev/null || true)
    fi
    [ -n "$npm_url" ] && [ "$npm_url" != "undefined" ] || npm_url="https://registry.npmjs.org/"

    if ! command -v curl >/dev/null 2>&1; then
        row "Python registry" "SKIP" "curl not available to probe"
        row "npm registry" "SKIP" "curl not available to probe"
    else
        if probe_url "$pypi_url"; then
            row "Python registry" "OK" "$pypi_url reachable"
        else
            row "Python registry" "BLOCKED" "$pypi_url unreachable — ask the user for their org's index URL (UV_INDEX_URL / PIP_INDEX_URL)"
        fi
        if probe_url "$npm_url"; then
            row "npm registry" "OK" "$npm_url reachable"
        else
            row "npm registry" "BLOCKED" "$npm_url unreachable — ask the user for their org's registry URL (npm config set registry <url>)"
        fi
    fi

    if [ "$blocking_failure" -ne 0 ]; then
        echo >&2
        echo "preflight: blocking check failed (see FAIL rows above)." >&2
        return 1
    fi
    return 0
}

# -------------------------------------------------------------- materialize

sed_escape() { # escape a string for use in a sed replacement (delimiter /)
    printf '%s' "$1" | sed -e 's/[&/\]/\\&/g'
}

is_three_part_id() { # catalog.schema.model — exactly three non-empty dot parts
    printf '%s' "$1" | awk -F. 'NF == 3 && $1 != "" && $2 != "" && $3 != "" { ok = 1 } END { exit ok ? 0 : 1 }'
}

cmd_materialize() {
    local agent="" host="" model="" out="" reason="" allow_legacy=0
    local profile="${DATABRICKS_CONFIG_PROFILE:-DEFAULT}"

    while [ $# -gt 0 ]; do
        case "$1" in
            --out)          [ $# -ge 2 ] || die "--out requires a path"; out="$2"; shift 2 ;;
            --profile)      [ $# -ge 2 ] || die "--profile requires a value"; profile="$2"; shift 2 ;;
            --allow-legacy) allow_legacy=1; shift ;;
            --reason)       [ $# -ge 2 ] || die "--reason requires a string"; reason="$2"; shift 2 ;;
            -h|--help)      usage; return 0 ;;
            -*)             die "unknown flag for materialize: $1" ;;
            *)
                if   [ -z "$agent" ]; then agent="$1"
                elif [ -z "$host"  ]; then host="$1"
                elif [ -z "$model" ]; then model="$1"
                else die "unexpected extra argument: $1"
                fi
                shift ;;
        esac
    done

    [ -n "$agent" ] && [ -n "$host" ] && [ -n "$model" ] \
        || die "usage: materialize <agent> <workspace-host> <model> [--profile <p>] [--out <path>] [--allow-legacy --reason \"<why>\"]"

    local template kind
    case "$agent" in
        claude-code)    template="$CONFIGS_DIR/claude-code.settings.json"; kind=json ;;
        claude-desktop) template="$CONFIGS_DIR/claude-desktop.mcp.json";   kind=json ;;
        codex)          template="$CONFIGS_DIR/codex.config.toml";         kind=toml ;;
        opencode)       template="$CONFIGS_DIR/opencode.json";             kind=json ;;
        *) die "unknown agent '$agent' (expected: claude-code | claude-desktop | codex | opencode)" ;;
    esac
    [ -f "$template" ] || die "template not found: $template"

    # Never a PAT, anywhere — not even smuggled in as an argument.
    case "$host$model$reason" in
        *dapi*) die "an argument looks like it contains a static PAT (dapi...). SSO-only — refused." ;;
    esac

    # Normalize host to a bare hostname (templates carry their own https://).
    local bare_host
    bare_host=$(printf '%s' "$host" | sed -e 's|^https\{0,1\}://||' -e 's|/*$||')
    [ -n "$bare_host" ] || die "workspace host is empty after normalization"

    # Legacy model-name gate.
    local annotation=""
    if ! is_three_part_id "$model"; then
        if [ "$allow_legacy" -ne 1 ]; then
            die "model '$model' is not a three-part Unity Catalog identifier (catalog.schema.model, e.g. system.ai.claude-sonnet-4-6).
Legacy single-string endpoint names are refused by default. If 'make verify' proved three-part
ids do not resolve on this workspace, re-run with: --allow-legacy --reason \"<why>\""
        fi
        [ -n "$reason" ] || die "--allow-legacy requires --reason \"<why>\" — the reason is embedded in the output as an annotation"
        # Keep the annotation safe to embed in JSON string values.
        local safe_reason
        safe_reason=$(printf '%s' "$reason" | tr -d '"\\' | tr '\n' ' ')
        annotation="LEGACY MODEL NAME '$model' ALLOWED — reason: $safe_reason. Three-part ids do not resolve on this workspace yet (see experiment README row 2)."
    fi

    # Render: substitute every placeholder; retarget the template's example
    # three-part model id (any system.ai.* occurrence) to the requested model.
    local rendered
    rendered=$(sed -E \
        -e "s/<your-workspace-host>/$(sed_escape "$bare_host")/g" \
        -e "s/<your-databricks-cli-profile>/$(sed_escape "$profile")/g" \
        -e "s/<path-to-this-experiment>/$(sed_escape "$EXPERIMENT_ROOT")/g" \
        -e "s/system\.ai\.[A-Za-z0-9_.-]+/$(sed_escape "$model")/g" \
        "$template")

    if [ -n "$annotation" ]; then
        case "$kind" in
            toml)
                rendered=$(printf '# %s\n%s\n' "$annotation" "$rendered") ;;
            json)
                # JSON has no comments; embed as a top-level marker key.
                rendered=$(printf '%s\n' "$rendered" | awk -v note="$annotation" '
                    !done && /\{/ { print; printf "  \"_ALLOW_LEGACY_REASON\": \"%s\",\n", note; done = 1; next }
                    { print }') ;;
        esac
        echo "warning: legacy model name embedded with annotation — record this as a finding, not a silent downgrade." >&2
    fi

    if [ "$agent" = "claude-desktop" ]; then
        echo "note: <your-mcp-server-command> is intentionally left for you to fill in — see configs/claude-desktop.md (Desktop's chat model cannot be repointed; MCP is the escape hatch)." >&2
    fi

    if [ -n "$out" ]; then
        mkdir -p "$(dirname -- "$out")"
        printf '%s\n' "$rendered" > "$out"
        echo "wrote $out (from ${template#"$EXPERIMENT_ROOT"/})" >&2
    else
        printf '%s\n' "$rendered"
    fi
}

# ------------------------------------------------------------------- verify

cmd_verify() {
    exec make -C "$EXPERIMENT_ROOT" verify
}

# --------------------------------------------------------------------- main

main() {
    [ $# -ge 1 ] || { usage >&2; exit 2; }
    local cmd="$1"; shift
    case "$cmd" in
        preflight)   cmd_preflight "$@" ;;
        materialize) cmd_materialize "$@" ;;
        verify)      cmd_verify "$@" ;;
        -h|--help|help) usage ;;
        *) die "unknown subcommand '$cmd' (expected: preflight | materialize | verify)" ;;
    esac
}

main "$@"
