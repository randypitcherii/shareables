"""Structured, copy-pasteable setup errors.

Every failure the setup job can explain becomes a SetupError with a stable code
(RPW-LW-E###). The rendered message is written for an admin who will paste it
into an AI assistant (Genie Code, Claude, ChatGPT, Codex, ...). It is
self-contained: what happened, the likely cause, the next steps, and the facts
the assistant needs.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

BUNDLE = "lakewatch-getting-started"
BEGIN = "=" * 20 + " COPY FROM HERE INTO YOUR AI ASSISTANT " + "=" * 20
END = "=" * 30 + " COPY TO HERE " + "=" * 30

# code -> short title. Codes are stable: docs and support searches key on them.
CODES = {
    "RPW-LW-E001": "Run as a workspace admin",
    "RPW-LW-E002": "Unity Catalog is not enabled for this workspace",
    "RPW-LW-E101": "An earlier Lakewatch preview setup was found; the Beta is not set up",
    "RPW-LW-E102": "Could not turn on the Lakewatch Beta preview",
    "RPW-LW-E103": "Lakewatch Beta is on, but the Lakewatch service is not reachable yet",
    "RPW-LW-E104": "Lakewatch is already set up on a different catalog",
    "RPW-LW-E201": "The metastore has no default storage for a new catalog",
    "RPW-LW-E202": "Could not create the Lakewatch catalog",
    "RPW-LW-E203": "Could not register the Lakewatch environment",
    "RPW-LW-E204": "Lakewatch initialization did not complete",
    "RPW-LW-E205": "Could not create the rpw_ folder (schema) for detections",
    "RPW-LW-E301": "Posture collection could not write its tables",
    "RPW-LW-E401": "Some detection rules could not be registered",
    "RPW-LW-E999": "Unexpected error",
}


@dataclass
class SetupError(Exception):
    code: str
    what_happened: str
    likely_cause: str
    next_steps: list[str]
    context: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.code not in CODES:
            raise ValueError(f"unknown error code {self.code}")
        super().__init__(f"{self.code}: {CODES[self.code]}")

    @property
    def title(self) -> str:
        return CODES[self.code]

    def render(self) -> str:
        steps = "\n".join(f"  {i}. {s}" for i, s in enumerate(self.next_steps, 1))
        ctx = json.dumps(self.context, indent=2, default=str, sort_keys=True)
        return f"""
{BEGIN}
I'm setting up Databricks Lakewatch (Beta) in my workspace with the open-source
"{BUNDLE}" Databricks Asset Bundle, and its setup job stopped with
error {self.code}: {self.title}.

What happened:
  {self.what_happened}

Likely cause:
  {self.likely_cause}

Suggested next steps from the bundle:
{steps}

Facts about my environment (from the job run):
{ctx}

Please help me fix this step by step. Ask me for anything you need to check
(for example, output of a Databricks CLI command or a screenshot of a settings
page). The setup job is safe to re-run after each fix.
{END}
"""


def unexpected(step: str, exc: BaseException, context: dict | None = None) -> SetupError:
    return SetupError(
        "RPW-LW-E999",
        what_happened=f"Step '{step}' raised {type(exc).__name__}: {str(exc)[:800]}",
        likely_cause="Not a case the bundle recognizes. It is often a permission, a workspace feature that is "
                     "turned off, or a temporary service error.",
        next_steps=[
            "Re-run the job once; temporary errors usually clear.",
            "If it fails again, paste this whole block into your AI assistant along with the job run's full log.",
        ],
        context={**(context or {}), "step": step},
    )
