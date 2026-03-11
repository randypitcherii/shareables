from __future__ import annotations

import pytest
from unittest.mock import MagicMock
from server.alias_registry import AliasRegistry, parse_model_family


class TestParseModelFamily:
    def test_claude_sonnet(self):
        family, version = parse_model_family("databricks-claude-sonnet-4-5")
        assert family == "claude-sonnet"
        assert version == (4, 5)

    def test_claude_opus(self):
        family, version = parse_model_family("databricks-claude-opus-4-6")
        assert family == "claude-opus"
        assert version == (4, 6)

    def test_gpt_4o(self):
        family, version = parse_model_family("databricks-gpt-4o")
        assert family == "gpt-4o"

    def test_gpt_4o_mini(self):
        family, version = parse_model_family("databricks-gpt-4o-mini")
        assert family == "gpt-4o-mini"

    def test_gpt_4o_and_mini_are_separate_families(self):
        f1, _ = parse_model_family("databricks-gpt-4o")
        f2, _ = parse_model_family("databricks-gpt-4o-mini")
        assert f1 != f2

    def test_gemini_pro(self):
        family, version = parse_model_family("databricks-gemini-2-0-flash")
        assert family == "gemini"
        assert version == (2, 0)

    def test_codex(self):
        family, version = parse_model_family("databricks-codex-mini-latest")
        assert family == "codex"

    def test_gpt_5(self):
        family, version = parse_model_family("databricks-gpt-5")
        assert family == "gpt"
        assert version == (5,)

    def test_gpt_5_turbo(self):
        family, version = parse_model_family("databricks-gpt-5-turbo")
        assert family == "gpt"
        assert version == (5,)

    def test_gpt_5_beats_gpt_4_for_gpt_latest(self):
        """gpt-5 should win over gpt-4 for gpt-latest alias."""
        f1, v1 = parse_model_family("databricks-gpt-4")
        f2, v2 = parse_model_family("databricks-gpt-5")
        assert f1 == f2 == "gpt"
        assert v2 > v1

    def test_gpt_5_does_not_compete_with_gpt_4o(self):
        """gpt-5 and gpt-4o are separate families."""
        f1, _ = parse_model_family("databricks-gpt-5")
        f2, _ = parse_model_family("databricks-gpt-4o")
        assert f1 == "gpt"
        assert f2 == "gpt-4o"
        assert f1 != f2

    def test_no_match(self):
        result = parse_model_family("my-custom-endpoint")
        assert result is None

    def test_with_date_suffix(self):
        family, version = parse_model_family("databricks-claude-sonnet-4-5-20260215")
        assert family == "claude-sonnet"
        assert version == (4, 5)


class TestAliasRegistry:
    def _mock_endpoints(self, names: list[str]) -> list:
        endpoints = []
        for name in names:
            ep = MagicMock()
            ep.name = name
            ep.state = MagicMock()
            ep.state.ready = "READY"
            endpoints.append(ep)
        return endpoints

    def test_resolve_latest_picks_highest_version(self):
        registry = AliasRegistry(prefixes=["databricks-claude", "databricks-gpt"])
        endpoints = self._mock_endpoints([
            "databricks-claude-sonnet-4-5",
            "databricks-claude-sonnet-4-6",
            "databricks-claude-opus-4-6",
            "databricks-gpt-4o",
        ])
        registry.refresh_from_endpoints(endpoints)

        assert registry.resolve("claude-sonnet-latest") == "databricks-claude-sonnet-4-6"
        assert registry.resolve("claude-opus-latest") == "databricks-claude-opus-4-6"
        assert registry.resolve("gpt-4o-latest") == "databricks-gpt-4o"

    def test_gpt_latest_picks_highest_gpt_version(self):
        registry = AliasRegistry(prefixes=["databricks-gpt"])
        endpoints = self._mock_endpoints([
            "databricks-gpt-4",
            "databricks-gpt-4o",
            "databricks-gpt-4o-mini",
            "databricks-gpt-5",
        ])
        registry.refresh_from_endpoints(endpoints)
        # gpt-5 wins gpt-latest, gpt-4o and gpt-4o-mini get their own aliases
        assert registry.resolve("gpt-latest") == "databricks-gpt-5"
        assert registry.resolve("gpt-4o-latest") == "databricks-gpt-4o"
        assert registry.resolve("gpt-4o-mini-latest") == "databricks-gpt-4o-mini"

    def test_resolve_passthrough(self):
        registry = AliasRegistry(prefixes=["databricks-claude"])
        registry.refresh_from_endpoints([])
        assert registry.resolve("my-custom-model") == "my-custom-model"

    def test_resolve_explicit_endpoint_name(self):
        registry = AliasRegistry(prefixes=["databricks-claude"])
        endpoints = self._mock_endpoints(["databricks-claude-sonnet-4-5"])
        registry.refresh_from_endpoints(endpoints)
        assert registry.resolve("databricks-claude-sonnet-4-5") == "databricks-claude-sonnet-4-5"

    def test_list_aliases(self):
        registry = AliasRegistry(prefixes=["databricks-claude"])
        endpoints = self._mock_endpoints([
            "databricks-claude-sonnet-4-5",
            "databricks-claude-opus-4-6",
        ])
        registry.refresh_from_endpoints(endpoints)
        aliases = registry.list_aliases()
        assert "claude-sonnet-latest" in aliases
        assert "claude-opus-latest" in aliases

    def test_list_available_endpoints(self):
        registry = AliasRegistry(prefixes=["databricks-claude"])
        endpoints = self._mock_endpoints(["databricks-claude-sonnet-4-5"])
        registry.refresh_from_endpoints(endpoints)
        available = registry.list_endpoints()
        assert "databricks-claude-sonnet-4-5" in available
