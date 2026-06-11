#!/usr/bin/env bash
# Supervises an ephemeral GitHub Actions runner inside the Databricks App container.
#
# Each cycle: (re)register with --ephemeral, run one job, repeat. A registration
# token is valid for ~1 hour and may register multiple runners, so a manually
# pasted token keeps the loop self-healing for that window. A production setup
# would mint fresh tokens from a GitHub App or fine-grained PAT instead.
#
# Required env:
#   GH_RUNNER_REG_TOKEN  registration token from
#                        POST /repos/{owner}/{repo}/actions/runners/registration-token
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
REG_TOKEN="${GH_RUNNER_REG_TOKEN:?GH_RUNNER_REG_TOKEN is required}"

log() { printf '%s supervisor: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }

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
    log "registration failed (token likely expired after ~1h) — exiting"
    exit 1
  fi
  log "listening for jobs"
  ./run.sh
  log "runner exited (ephemeral job complete or shutdown) — re-registering"
  sleep 2
done
