from __future__ import annotations

import asyncio
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Family patterns – order matters: first match wins.
# Each entry: (prefix_to_strip, family_name, regex applied to the remainder)
# ---------------------------------------------------------------------------
_FAMILY_PATTERNS: list[tuple[str, str, re.Pattern]] = [
    ("databricks-claude-sonnet-", "claude-sonnet", re.compile(r"^(\d+)(?:[-.](\d+))?(?:-\d{8})?$")),
    ("databricks-claude-opus-", "claude-opus", re.compile(r"^(\d+)(?:[-.](\d+))?(?:-\d{8})?$")),
    ("databricks-claude-haiku-", "claude-haiku", re.compile(r"^(\d+)(?:[-.](\d+))?(?:-\d{8})?$")),
    ("databricks-gemini-", "gemini", re.compile(r"^(\d+)[-.](\d+)")),
    ("databricks-codex-", "codex", re.compile(r"^(.+)$")),
    ("databricks-gpt-4o-mini", "gpt-4o-mini", re.compile(r"^(?:-(\d{4}-\d{2}-\d{2}))?$")),
    ("databricks-gpt-4o", "gpt-4o", re.compile(r"^(?:-(\d{4}-\d{2}-\d{2}))?$")),
    ("databricks-gpt-", "gpt", re.compile(r"^(\d+)(?:[-.](\d+))?")),
]


def parse_model_family(endpoint_name: str) -> Optional[tuple[str, tuple[int, ...]]]:
    """Parse an endpoint name into (family, version_tuple) or None."""
    for prefix, family, pattern in _FAMILY_PATTERNS:
        if not endpoint_name.startswith(prefix):
            continue
        remainder = endpoint_name[len(prefix):]
        m = pattern.match(remainder)
        if m is None:
            continue
        # Build a version tuple from captured numeric groups
        version_parts: list[int] = []
        for g in m.groups():
            if g is not None:
                try:
                    version_parts.append(int(g))
                except ValueError:
                    pass  # non-numeric capture (e.g. date string or codex suffix)
        return family, tuple(version_parts)
    return None


class AliasRegistry:
    """Discovers serving endpoints and builds ``<family>-latest`` aliases."""

    def __init__(self, prefixes: list[str] | None = None) -> None:
        self._prefixes = prefixes or ["databricks-"]
        # family -> (best_version_tuple, endpoint_name)
        self._best: dict[str, tuple[tuple[int, ...], str]] = {}
        # alias -> endpoint_name
        self._aliases: dict[str, str] = {}
        # set of discovered endpoint names
        self._endpoints: set[str] = set()

    # ------------------------------------------------------------------
    # Core logic
    # ------------------------------------------------------------------

    def refresh_from_endpoints(self, endpoints) -> None:
        """Rebuild aliases from a list of endpoint objects (or dicts)."""
        self._best.clear()
        self._aliases.clear()
        self._endpoints.clear()

        endpoint_count = 0
        prefix_match_count = 0
        ready_count = 0
        for ep in endpoints:
            endpoint_count += 1
            name = ep.name if hasattr(ep, "name") else ep.get("name", "")
            # Only consider endpoints matching configured prefixes
            if not any(name.startswith(p) for p in self._prefixes):
                continue
            prefix_match_count += 1

            # Only consider READY endpoints
            ready = None
            if hasattr(ep, "state"):
                ready = getattr(ep.state, "ready", None)
            elif isinstance(ep, dict):
                ready = (ep.get("state") or {}).get("ready")
            ready_str = ready.value if hasattr(ready, "value") else str(ready)
            if ready_str != "READY":
                logger.info("Skipping %s: ready=%r", name, ready)
                continue
            ready_count += 1

            self._endpoints.add(name)

            parsed = parse_model_family(name)
            if parsed is None:
                logger.info("No family match for endpoint: %s", name)
                continue

            family, version = parsed
            logger.info("Matched %s -> family=%s version=%s", name, family, version)
            current = self._best.get(family)
            if current is None or version > current[0]:
                self._best[family] = (version, name)

        logger.info("Refresh stats: %d total, %d prefix-matched, %d ready, %d family-matched",
                    endpoint_count, prefix_match_count, ready_count, len(self._best))

        # Build alias map
        for family, (_, endpoint_name) in self._best.items():
            alias = f"{family}-latest"
            self._aliases[alias] = endpoint_name
            logger.info("Alias %s -> %s", alias, endpoint_name)

    def resolve(self, alias_or_model: str) -> str:
        """Resolve an alias to an endpoint name, or pass through unchanged."""
        return self._aliases.get(alias_or_model, alias_or_model)

    def list_aliases(self) -> dict[str, str]:
        """Return a copy of the current alias -> endpoint mapping."""
        return dict(self._aliases)

    def list_endpoints(self) -> list[str]:
        """Return sorted list of discovered endpoint names."""
        return sorted(self._endpoints)

    # ------------------------------------------------------------------
    # Workspace discovery (async-friendly)
    # ------------------------------------------------------------------

    async def refresh_from_workspace(self, workspace_client) -> None:
        """Fetch serving endpoints from workspace and rebuild aliases."""
        endpoints = await asyncio.to_thread(
            lambda: list(workspace_client.serving_endpoints.list()),
        )
        self.refresh_from_endpoints(endpoints)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
_registry: AliasRegistry | None = None


def get_registry() -> AliasRegistry:
    """Return the module-level singleton, creating it if needed."""
    global _registry
    if _registry is None:
        _registry = AliasRegistry()
    return _registry


async def refresh_registry_from_workspace() -> None:
    """Refresh the module-level registry from the workspace."""
    from .config import get_workspace_client, settings
    registry = get_registry()
    try:
        client = get_workspace_client()
        await registry.refresh_from_workspace(client)
        logger.info("Registry refreshed successfully")
    except Exception:
        logger.exception("Failed to refresh alias registry from workspace")
