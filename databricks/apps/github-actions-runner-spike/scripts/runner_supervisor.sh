#!/usr/bin/env bash
# Supervises an ephemeral GitHub Actions runner inside the Databricks App container.
#
# Each cycle: (re)register with --ephemeral, run one job, repeat.
#
# Auth — provide ONE of:
#   GH_RUNNER_PAT        fine-grained PAT (repo `administration:write`) stored in a
#                        Databricks secret and injected into the app env. The
#                        supervisor mints a FRESH registration token every cycle,
#                        so the loop is fully self-healing across app restarts and
#                        token expiry. This is the production mode.
#   GH_RUNNER_REG_TOKEN  pre-minted registration token from
#                        POST /repos/{owner}/{repo}/actions/runners/registration-token.
#                        Valid ~1 hour and reusable within that window; when it
#                        expires the supervisor exits. Spike/manual mode.
#
# Optional env:
#   GH_RUNNER_REPO_URL   default https://github.com/randypitcherii/shareables
#   GH_RUNNER_NAME       default gha-runner-spike
#   GH_RUNNER_LABELS     default databricks-app
#   GH_RUNNER_VERSION    default 2.335.1
#   RUNNER_HOME          default /home/app/gha-runner
set -uo pipefail

REPO_URL="${GH_RUNNER_REPO_URL:-https://github.com/randypitcherii/shareables}"
RUNNER_NAME="${GH_RUNNER_NAME:-gha-runner-spike}"
RUNNER_LABELS="${GH_RUNNER_LABELS:-databricks-app}"
RUNNER_VERSION="${GH_RUNNER_VERSION:-2.335.1}"
RUNNER_HOME="${RUNNER_HOME:-/home/app/gha-runner}"
ICU_HOME="${ICU_HOME:-/home/app/icu}"
RUNNER_PAT="${GH_RUNNER_PAT:-}"
REG_TOKEN="${GH_RUNNER_REG_TOKEN:-}"
REPO_SLUG="${REPO_URL#https://github.com/}"

log() { printf '%s supervisor: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }

if [ -z "${RUNNER_PAT}" ] && [ -z "${REG_TOKEN}" ]; then
  log "one of GH_RUNNER_PAT or GH_RUNNER_REG_TOKEN is required"
  exit 1
fi

mint_registration_token() {
  curl -fsS -X POST \
    -H "Authorization: Bearer ${RUNNER_PAT}" \
    -H "Accept: application/vnd.github+json" \
    "https://api.github.com/repos/${REPO_SLUG}/actions/runners/registration-token" \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["token"])'
}

# --- one-time bootstrap: runner binary + libicu (image has none, and we are not root) ---
if [ ! -x "${RUNNER_HOME}/run.sh" ]; then
  log "downloading actions runner v${RUNNER_VERSION}"
  mkdir -p "${RUNNER_HOME}"
  curl -fsSL -o "${RUNNER_HOME}/runner.tar.gz" \
    "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz"
  tar -xzf "${RUNNER_HOME}/runner.tar.gz" -C "${RUNNER_HOME}"
  rm -f "${RUNNER_HOME}/runner.tar.gz"
fi

if [ ! -e "${ICU_HOME}/usr/lib/x86_64-linux-gnu/libicuuc.so.70" ]; then
  log "extracting libicu70 into ${ICU_HOME} (user-space, no root needed)"
  mkdir -p "${ICU_HOME}"
  cd "${ICU_HOME}"
  curl -fsSL -o libicu70.deb \
    "http://archive.ubuntu.com/ubuntu/pool/main/i/icu/libicu70_70.1-2_amd64.deb"
  dpkg-deb -x libicu70.deb . 2>/dev/null || { ar x libicu70.deb && tar -xf data.tar.*; }
  rm -f libicu70.deb control.tar.* data.tar.* debian-binary
fi

export LD_LIBRARY_PATH="${ICU_HOME}/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
cd "${RUNNER_HOME}"

# --- supervisor loop: ephemeral runners exit after one job, so re-register each cycle ---
while true; do
  if [ -n "${RUNNER_PAT}" ]; then
    # Assign via a temp var: `REG_TOKEN=$(...)` would clobber a manually
    # provided GH_RUNNER_REG_TOKEN even when minting fails.
    if MINTED_TOKEN=$(mint_registration_token); then
      REG_TOKEN="${MINTED_TOKEN}"
    elif [ -n "${REG_TOKEN}" ]; then
      log "PAT mint failed — falling back to the provided registration token"
    else
      log "failed to mint a registration token (GitHub API unreachable or PAT invalid) — retrying in 60s"
      sleep 60
      continue
    fi
  fi

  rm -f .runner .credentials .credentials_rsaparams
  log "registering ephemeral runner '${RUNNER_NAME}' against ${REPO_URL}"
  if ! ./config.sh \
      --url "${REPO_URL}" \
      --token "${REG_TOKEN}" \
      --name "${RUNNER_NAME}" \
      --labels "${RUNNER_LABELS}" \
      --ephemeral \
      --unattended \
      --replace; then
    if [ -n "${RUNNER_PAT}" ]; then
      log "registration failed — retrying with a fresh token in 60s"
      sleep 60
      continue
    fi
    log "registration failed (token likely expired after ~1h) — exiting"
    exit 1
  fi
  log "listening for jobs"
  ./run.sh
  log "runner exited (ephemeral job complete or shutdown) — re-registering"
  sleep 2
done
