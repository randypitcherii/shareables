"""Unit tests for the event generator: size, mix, heterogeneity, and constant sync."""

import importlib.util
import json
import random
import re
from pathlib import Path

EXPERIMENT_ROOT = Path(__file__).resolve().parent.parent


def _load_generator():
    path = EXPERIMENT_ROOT / "scripts" / "00_generate_events.py"
    spec = importlib.util.spec_from_file_location("generate_events", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


GEN = _load_generator()


def _sample(n=2000, seed=42):
    rng = random.Random(seed)
    return [json.loads(GEN.build_event(rng, i)) for i in range(n)]


def test_average_size_is_about_1kb():
    rng = random.Random(7)
    sizes = [len(GEN.build_event(rng, i)) for i in range(2000)]
    avg = sum(sizes) / len(sizes)
    assert 700 <= avg <= 1400, f"average event size {avg:.0f}B is not ~1KB"


def test_noise_fraction_matches_design():
    events = _sample()
    noise = sum(1 for e in events if e["event_type"] in GEN.NOISE_EVENT_TYPES)
    frac = noise / len(events)
    assert 0.55 <= frac <= 0.75, f"noise fraction {frac:.2f} out of design range (~0.65)"


def test_weights_sum_to_one_and_cover_all_types():
    assert abs(sum(GEN.EVENT_TYPE_WEIGHTS.values()) - 1.0) < 1e-9
    assert set(GEN.EVENT_TYPE_WEIGHTS) == set(GEN.KEEP_EVENT_TYPES) | set(GEN.NOISE_EVENT_TYPES)


def test_payloads_are_nested_and_heterogeneous():
    events = _sample(4000)
    by_type = {}
    for e in events:
        by_type.setdefault(e["event_type"], []).append(e)

    purchases = by_type.get("purchase", [])
    assert purchases, "sample contains no purchases"
    assert any(
        isinstance(p["properties"]["items"], list) and isinstance(p["properties"]["items"][0], dict)
        for p in purchases
    ), "purchase items must be arrays of objects"

    bounces = by_type.get("bounce", [])
    codes = {type(b["properties"]["smtp_code"]).__name__ for b in bounces}
    assert len(codes) > 1, f"smtp_code should be mixed-type, saw only {codes}"

    schema_versions = {type(e["schema_version"]).__name__ for e in events}
    assert len(schema_versions) > 1, "schema_version should mix int and str"

    # optional keys genuinely absent sometimes
    has_city = [e for e in events if "city" in e["context"]["geo"]]
    assert 0 < len(has_city) < len(events), "geo.city should be optional"


def test_tenant_skew_is_zipf_ish():
    events = _sample(5000)
    counts = {}
    for e in events:
        counts[e["project_id"]] = counts.get(e["project_id"], 0) + 1
    top = max(counts.values())
    assert top / len(events) > 0.05, "hottest tenant should carry a meaningful share"
    assert len(counts) > 50, "long tail of tenants expected"


def test_keep_types_stay_in_sync_across_files():
    """The keep-set is duplicated in three standalone files by design; they must match."""
    expected = set(GEN.KEEP_EVENT_TYPES)

    common = (EXPERIMENT_ROOT / "scripts" / "_common.py").read_text()
    spark = (EXPERIMENT_ROOT / "databricks" / "src" / "spark_ingest.py").read_text()
    for name, text in [("_common.py", common), ("spark_ingest.py", spark)]:
        m = re.search(r"KEEP_EVENT_TYPES\s*=\s*\(([^)]*)\)", text)
        assert m, f"KEEP_EVENT_TYPES not found in {name}"
        found = set(re.findall(r'"(\w+)"', m.group(1)))
        assert found == expected, f"{name} keep-set {found} != generator {expected}"
