"""Run Omnigent's native Claude terminal through Unity Gateway's `ug claude --provider`.

Omnigent launches the native Claude terminal as `claude <args>` and lets an installed plugin
rewrite that launch (entry-point group `omnigent.claude_launcher`, selected with
`OMNIGENT_CLAUDE_LAUNCHER`). This plugin turns it into::

    ug claude --provider <catalog.schema.service> [--workspace <url>] <every arg Omnigent added>

so the session's Claude Code relays the host owner's own Claude subscription through one
relayed model provider service, with `ug` refreshing the Databricks token per request.

Configure it on each Omnigent host (the host owner's subscription decides which service):

    OMNIGENT_CLAUDE_LAUNCHER=ug
    UG_CLAUDE_PROVIDER=main.multi_claude_sub.claude_sub_a   # this host owner's subscription
    UG_WORKSPACE=https://<workspace-url>                    # optional; ug's own default otherwise
    OMNIGENT_RUNNER_ENV_PASSTHROUGH=UG_CLAUDE_PROVIDER,UG_WORKSPACE

The last line matters on a managed host: the daemon strips unknown env vars before the runner
spawns the terminal. With no provider configured the launch is left unchanged, so a
misconfigured host falls back to plain `claude` instead of failing.
"""
from __future__ import annotations

import os
import shutil

try:  # Omnigent only loads launchers that subclass its ABC; tests run without it.
    from omnigent.claude_launcher import ClaudeLauncher as _Base
except ImportError:  # pragma: no cover - exercised only when omnigent is absent
    class _Base:  # type: ignore[no-redef]
        """Stand-in so the plugin imports (and unit-tests) without Omnigent installed."""

PROVIDER_ENV = "UG_CLAUDE_PROVIDER"
WORKSPACE_ENV = "UG_WORKSPACE"
UG_BINARY_ENV = "UG_BINARY"


def _three_part(name: str) -> bool:
    parts = name.split(".")
    return len(parts) == 3 and all(parts)


class UgClaudeLauncher(_Base):
    """Wrap `claude <args>` as `ug claude --provider <service> [--workspace <url>] <args>`."""

    def launch(self, command: str, args: list[str]) -> tuple[str, list[str]]:
        provider = os.environ.get(PROVIDER_ENV, "").strip()
        if not provider:
            return command, list(args)
        if not _three_part(provider):
            raise ValueError(f"{PROVIDER_ENV} must be <catalog>.<schema>.<service>, got {provider!r}")
        ug = os.environ.get(UG_BINARY_ENV, "").strip() or ("ug" if shutil.which("ug") or not shutil.which("ucode")
                                                            else "ucode")
        wrapped = ["claude", "--provider", provider]
        workspace = os.environ.get(WORKSPACE_ENV, "").strip()
        if workspace:
            wrapped += ["--workspace", workspace]
        # ug forwards options it does not own to Claude Code verbatim; ug's own --provider and
        # --workspace must come first.
        return ug, [*wrapped, *args]
