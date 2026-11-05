import pytest

from omnigent_ug_launcher import UgClaudeLauncher

OMNI_ARGS = ["--mcp-config", "/tmp/mcp.json", "--settings", "/tmp/hooks.json", "--session-id", "abc"]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for k in ("UG_CLAUDE_PROVIDER", "UG_WORKSPACE", "UG_BINARY"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("UG_BINARY", "ug")


def test_no_provider_leaves_launch_unchanged():
    assert UgClaudeLauncher().launch("claude", OMNI_ARGS) == ("claude", OMNI_ARGS)


def test_wraps_with_provider_and_keeps_every_omnigent_arg(monkeypatch):
    monkeypatch.setenv("UG_CLAUDE_PROVIDER", "main.multi_claude_sub.claude_sub_b")
    cmd, args = UgClaudeLauncher().launch("claude", OMNI_ARGS)
    assert cmd == "ug"
    assert args == ["claude", "--provider", "main.multi_claude_sub.claude_sub_b", *OMNI_ARGS]


def test_workspace_goes_before_forwarded_args(monkeypatch):
    monkeypatch.setenv("UG_CLAUDE_PROVIDER", "main.s.claude_sub_a")
    monkeypatch.setenv("UG_WORKSPACE", "https://example.cloud.databricks.com")
    _, args = UgClaudeLauncher().launch("claude", OMNI_ARGS)
    assert args[:5] == ["claude", "--provider", "main.s.claude_sub_a", "--workspace",
                        "https://example.cloud.databricks.com"]
    assert args[5:] == OMNI_ARGS


def test_does_not_mutate_input(monkeypatch):
    monkeypatch.setenv("UG_CLAUDE_PROVIDER", "main.s.claude_sub_a")
    original = list(OMNI_ARGS)
    UgClaudeLauncher().launch("claude", OMNI_ARGS)
    assert OMNI_ARGS == original


@pytest.mark.parametrize("bad", ["claude_sub_a", "main.claude_sub_a", "a..b", "a.b.c.d"])
def test_rejects_non_three_part_provider(monkeypatch, bad):
    # Omnigent logs a raising launcher and falls back to plain `claude`.
    monkeypatch.setenv("UG_CLAUDE_PROVIDER", bad)
    with pytest.raises(ValueError):
        UgClaudeLauncher().launch("claude", [])


def test_entry_point_registered():
    import importlib.metadata as md

    names = {e.name: e.value for e in md.entry_points(group="omnigent.claude_launcher")}
    assert names.get("ug") == "omnigent_ug_launcher:UgClaudeLauncher"
