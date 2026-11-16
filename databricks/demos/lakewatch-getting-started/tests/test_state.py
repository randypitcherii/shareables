import pytest

from rpw_lakewatch.state import Action, Observed, decide


def obs(**kw):
    base = dict(beta_setting=False, environments=None, prior_preview_setup=False, target_catalog="rpw_lakewatch")
    base.update(kw)
    return Observed(**base)


def test_fresh_workspace_turns_beta_on_and_creates_environment():
    p = decide(obs())
    assert p.action is Action.CREATE_ENVIRONMENT and p.enable_beta and p.catalog == "rpw_lakewatch"


def test_beta_on_but_no_environment_creates_without_toggling():
    p = decide(obs(beta_setting=True, environments=[]))
    assert p.action is Action.CREATE_ENVIRONMENT and not p.enable_beta


def test_unknown_toggle_attempts_enable():
    p = decide(obs(beta_setting=None))
    assert p.action is Action.CREATE_ENVIRONMENT and p.enable_beta and p.notes


def test_beta_fully_set_up_on_target_catalog_is_reused_unchanged():
    p = decide(obs(beta_setting=True, environments=["rpw_lakewatch"]))
    assert p.action is Action.USE_EXISTING and not p.enable_beta and p.catalog == "rpw_lakewatch"


def test_beta_set_up_with_prior_preview_still_reused():
    p = decide(obs(beta_setting=True, environments=["rpw_lakewatch"], prior_preview_setup=True))
    assert p.action is Action.USE_EXISTING


def test_existing_environment_with_toggle_off_is_not_toggled():
    p = decide(obs(beta_setting=False, environments=["rpw_lakewatch"]))
    assert p.action is Action.USE_EXISTING and not p.enable_beta and "leaving it off" in p.notes[0]


def test_existing_environment_on_other_catalog_stops():
    p = decide(obs(beta_setting=True, environments=["corp_security"]))
    assert p.action is Action.STOP and p.error_code == "RPW-LW-E104" and "corp_security" in p.notes[0]


@pytest.mark.parametrize("setting", [True, False, None])
def test_prior_preview_without_beta_stops(setting):
    p = decide(obs(beta_setting=setting, environments=[], prior_preview_setup=True))
    assert p.action is Action.STOP and p.error_code == "RPW-LW-E101"


def test_prior_preview_with_explicit_opt_in_proceeds():
    p = decide(obs(prior_preview_setup=True, allow_with_prior_preview=True))
    assert p.action is Action.CREATE_ENVIRONMENT and "untouched" in p.notes[0]


def test_unknown_prior_preview_does_not_block():
    assert decide(obs(prior_preview_setup=None)).action is Action.CREATE_ENVIRONMENT
