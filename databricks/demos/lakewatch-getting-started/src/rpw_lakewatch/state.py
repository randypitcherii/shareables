"""Pure decision logic for Lakewatch enablement. No I/O, so it is fully unit-tested.

Observed facts -> one Plan. The policy is opinionated but never destructive:

  1. Lakewatch Beta already set up (an environment exists)  -> reuse it, change nothing.
  2. Nothing set up and no earlier preview                   -> turn on the Beta, create the environment.
  3. An earlier (pre-Beta) Lakewatch preview setup exists,
     but no Beta environment                                  -> STOP and explain (RPW-LW-E101). Moving a
                                                                 preview workspace onto the Beta is the
                                                                 customer's decision, not this bundle's.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Action(str, Enum):
    USE_EXISTING = "use_existing"            # Beta environment exists on the target catalog
    CREATE_ENVIRONMENT = "create_environment"  # Beta toggle on (or turned on now); register a new environment
    STOP = "stop"                            # explain and exit without changes


@dataclass(frozen=True)
class Observed:
    beta_setting: bool | None          # workspace preview "lakewatch" effective value; None = could not read
    environments: list[str] | None     # catalogs with a Lakewatch environment; None = API unavailable
    prior_preview_setup: bool | None   # legacy workspace config exists; None = unknown
    target_catalog: str                # bundle variable `catalog`
    allow_with_prior_preview: bool = False


@dataclass
class Plan:
    action: Action
    catalog: str | None = None
    enable_beta: bool = False
    error_code: str | None = None
    notes: list[str] = field(default_factory=list)


def decide(o: Observed) -> Plan:
    envs = o.environments or []

    # State 1: the Beta is set up. Reuse it, never reconfigure it.
    if envs:
        if o.target_catalog in envs:
            notes = []
            if o.beta_setting is False:
                notes.append("Lakewatch environment exists but the Beta preview toggle is off; leaving it off. "
                             "Turn it on under Settings > Previews to see Lakewatch in the app switcher.")
            return Plan(Action.USE_EXISTING, catalog=o.target_catalog, notes=notes)
        return Plan(Action.STOP, error_code="RPW-LW-E104",
                    notes=[f"existing environments: {sorted(envs)}; bundle catalog: {o.target_catalog}"])

    # State 3: an earlier preview is configured and the Beta is not. Do not push it onto the Beta.
    if o.prior_preview_setup and not o.allow_with_prior_preview:
        return Plan(Action.STOP, error_code="RPW-LW-E101")

    # State 2: fresh. Turn the Beta on if needed, then create the environment.
    notes = []
    if o.prior_preview_setup:
        notes.append("Earlier preview setup found; continuing because allow_with_prior_preview=true. "
                     "Preview resources are left untouched.")
    if o.beta_setting is None:
        notes.append("Could not read the Lakewatch preview toggle; will try to turn it on.")
    return Plan(Action.CREATE_ENVIRONMENT, catalog=o.target_catalog,
                enable_beta=o.beta_setting is not True, notes=notes)
