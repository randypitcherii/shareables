"""Sanity checks on the run presets and the DAB wiring they depend on."""

import importlib.util
import re
import sys
from pathlib import Path

EXPERIMENT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(EXPERIMENT_ROOT / "scripts"))  # runner does `from _common import ...`


def _load(name, rel):
    path = EXPERIMENT_ROOT / rel
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


RUNNER = _load("runner", "scripts/01_run_databricks_cells.py")


def test_presets_reference_known_job_keys():
    for preset, (path, _cells) in RUNNER.PRESETS.items():
        assert path in RUNNER.JOB_KEYS, preset


def test_window_cells_allow_enough_commits():
    """A timed trigger needs window >= 2 intervals + slack, or p95 is meaningless."""
    for preset, (_path, cells) in RUNNER.PRESETS.items():
        for cell in cells:
            if cell["mode"] == "window" and cell.get("trigger_sec", 0) > 0:
                assert cell["window_sec"] >= 2 * cell["trigger_sec"] + 120, (
                    f"{preset}/{cell['name']}: window {cell['window_sec']}s too short "
                    f"for trigger {cell['trigger_sec']}s"
                )


def test_ladder_cells_start_from_latest():
    """Freshness cells must not chew pre-run backlog (backlog age != freshness)."""
    for _preset, (_path, cells) in RUNNER.PRESETS.items():
        for cell in cells:
            if cell["mode"] == "window":
                assert cell["starting"] == "latest", cell["name"]
            else:
                assert cell["starting"] == "earliest", cell["name"]


def test_cell_names_are_unique_across_presets():
    names = [c["name"] for _p, (_k, cells) in RUNNER.PRESETS.items() for c in cells]
    assert len(names) == len(set(names))


def test_dab_placeholder_arity_matches_job_script():
    """spark_ingest.main unpacks 7 argv values; the DAB placeholders must match."""
    yml = (EXPERIMENT_ROOT / "databricks" / "databricks.yml").read_text()
    for params in re.findall(r"parameters: \[(.*)\]", yml):
        assert len(params.split(",")) == 7, params
