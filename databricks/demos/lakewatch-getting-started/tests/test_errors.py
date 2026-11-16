import pytest

from rpw_lakewatch.errors import BEGIN, CODES, END, SetupError, unexpected


def test_render_is_self_contained():
    e = SetupError("LWGS-E201", "it failed", "no storage", ["do a", "do b"], {"workspace_id": "1"})
    out = e.render()
    assert BEGIN in out and END in out and "LWGS-E201" in out
    assert "1. do a" in out and "2. do b" in out and '"workspace_id": "1"' in out
    assert str(e).startswith("LWGS-E201:")


def test_unknown_code_rejected():
    with pytest.raises(ValueError):
        SetupError("LWGS-E000", "", "", [])


def test_unexpected_wraps_step():
    e = unexpected("folder", KeyError("x"), {"a": 1})
    assert e.code == "LWGS-E999" and e.context["step"] == "folder"


def test_codes_are_well_formed():
    assert all(c.startswith("LWGS-E") and len(c) == 9 for c in CODES)
