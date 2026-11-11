"""Pure (network-free) pieces of the probe: endpoints, ramp schedule, aggregation, analysis.

Kept separate from 01_probe.py so the unit tests can cover them without a workspace.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field

from _common import IRC_PATH, LEGACY_PATH

# ---------------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------------

ENDPOINTS = {
    # name: (api root, description)
    "config": (IRC_PATH, "GET /v1/config"),
    "list_tables": (IRC_PATH, "GET /v1/{prefix}/namespaces/{ns}/tables"),
    "load_table_hot": (IRC_PATH, "GET .../tables/{t} on one table, vended credentials"),
    "load_table_spread": (IRC_PATH, "GET .../tables/{t} round-robin over N tables, vended credentials"),
    "load_table_nocreds": (IRC_PATH, "GET .../tables/{t} round-robin, no credential vending"),
    "load_table_credentials": (IRC_PATH, "GET .../tables/{t}/credentials round-robin"),
    "legacy_config": (LEGACY_PATH, "GET legacy /iceberg /v1/config"),
    "legacy_load_table": (LEGACY_PATH, "GET legacy /iceberg .../tables/{t} (UniForm Delta)"),
}


@dataclass(frozen=True)
class Target:
    """Everything needed to build request URLs for one workspace."""

    host: str
    catalog: str
    schema: str
    tables: tuple[str, ...]
    prefix: str = ""  # from /v1/config overrides.prefix; defaults to catalogs/<catalog>
    legacy_prefix: str = ""
    legacy_table: str = "uniform_t000"

    def _prefix(self, legacy: bool) -> str:
        p = self.legacy_prefix if legacy else self.prefix
        return p or f"catalogs/{self.catalog}"


def build_request(endpoint: str, target: Target, seq: int) -> tuple[str, dict[str, str]]:
    """Return (url, extra headers) for the seq-th request of `endpoint`."""
    if endpoint not in ENDPOINTS:
        raise ValueError(f"unknown endpoint {endpoint!r}; choose from {sorted(ENDPOINTS)}")
    root, _ = ENDPOINTS[endpoint]
    base = f"{target.host}{root}/v1"
    legacy = endpoint.startswith("legacy_")
    ns = f"{base}/{target._prefix(legacy)}/namespaces/{target.schema}"
    vend = {"X-Iceberg-Access-Delegation": "vended-credentials"}
    if endpoint in ("config", "legacy_config"):
        return f"{base}/config?warehouse={target.catalog}", {}
    if endpoint == "list_tables":
        return f"{ns}/tables", {}
    if endpoint == "load_table_hot":
        return f"{ns}/tables/{target.tables[0]}", vend
    if endpoint == "load_table_spread":
        return f"{ns}/tables/{target.tables[seq % len(target.tables)]}", vend
    if endpoint == "load_table_nocreds":
        return f"{ns}/tables/{target.tables[seq % len(target.tables)]}", {}
    if endpoint == "load_table_credentials":
        return f"{ns}/tables/{target.tables[seq % len(target.tables)]}/credentials", {}
    if endpoint == "legacy_load_table":
        return f"{ns}/tables/{target.legacy_table}", {}
    raise AssertionError("unreachable")


# ---------------------------------------------------------------------------------
# Schedule
# ---------------------------------------------------------------------------------


@dataclass
class ProbePlan:
    """Bounded ramp -> hold -> drop/recovery plan for one endpoint.

    ramp: step through `rates`, `step_sec` each, until a step's 429 share reaches
          `onset_share` (then run `confirm_steps` more steps) or the rates run out.
    hold: stay at `hold_factor` x the onset rate for `hold_sec` (steady throttled state).
    recovery: drop to `recovery_rate` req/s for `recovery_sec` and watch the 429s stop.
    """

    rates: list[float]
    step_sec: float = 20.0
    onset_share: float = 0.05
    confirm_steps: int = 1
    hold_sec: float = 45.0
    hold_factor: float = 1.25
    recovery_rate: float = 2.0
    recovery_sec: float = 60.0
    max_rate: float = 1000.0
    max_duration_sec: float = 900.0

    def __post_init__(self) -> None:
        if not self.rates:
            raise ValueError("rates must be non-empty")
        if max(self.rates) > self.max_rate:
            raise ValueError(f"ramp peak {max(self.rates)} exceeds PROBE_MAX_RATE {self.max_rate}")
        if self.worst_case_duration() > self.max_duration_sec:
            raise ValueError(
                f"worst-case duration {self.worst_case_duration():.0f}s exceeds "
                f"PROBE_MAX_DURATION_SEC {self.max_duration_sec:.0f}s"
            )

    def worst_case_duration(self) -> float:
        return len(self.rates) * self.step_sec + self.hold_sec + self.recovery_sec

    def hold_rate(self, onset_rate: float) -> float:
        return min(self.max_rate, onset_rate * self.hold_factor)


def default_rates(endpoint: str) -> list[float]:
    """Ramp ladders sized to the reported limits (bounded; loadTable tops out at 1000/s)."""
    if endpoint == "load_table_credentials":  # reportedly 40/s at the edge, GTTC 80/s in UC
        return [5, 10, 20, 30, 40, 50, 60, 80, 100, 150, 200]
    if endpoint.startswith("load_table"):
        return [10, 25, 50, 100, 200, 250, 300, 350, 400, 500, 600, 800, 1000]
    if endpoint.startswith("legacy_"):
        return [1, 2, 3, 5, 8, 12, 20, 30, 45, 60]
    # config, list_tables: other IRC routes reportedly ~40/s; UC ListTables ~50/s
    return [5, 10, 20, 30, 40, 50, 60, 80, 100, 150, 200]


# ---------------------------------------------------------------------------------
# Records and aggregation
# ---------------------------------------------------------------------------------

# One call. status: HTTP status, 0 = transport error, -1 = skipped (client at
# max concurrency, so the attempt is still counted against the target rate).
RECORD_FIELDS = (
    "run_id",
    "workspace",
    "endpoint",
    "phase",
    "target_rate",
    "t_sent",
    "latency_ms",
    "status",
    "retry_after",
    "error_code",
    "body",
)


@dataclass
class Second:
    t: int
    attempted: int = 0
    ok: int = 0
    throttled: int = 0
    other: int = 0
    timeouts: int = 0
    skipped: int = 0
    latencies: list[float] = field(default_factory=list)
    target_rate: float = 0.0
    phase: str = ""

    def as_dict(self) -> dict:
        lat = sorted(self.latencies)
        return {
            "t": self.t,
            "attempted": self.attempted,
            "ok": self.ok,
            "throttled": self.throttled,
            "other": self.other,
            "timeouts": self.timeouts,
            "skipped": self.skipped,
            "target_rate": self.target_rate,
            "phase": self.phase,
            "p50_ms": _pct(lat, 0.5),
            "p95_ms": _pct(lat, 0.95),
        }


def _pct(sorted_vals: list[float], q: float) -> float | None:
    if not sorted_vals:
        return None
    idx = min(len(sorted_vals) - 1, max(0, math.ceil(q * len(sorted_vals)) - 1))
    return round(sorted_vals[idx], 1)


def per_second(records: list[dict], t0: float) -> list[dict]:
    """Bucket call records by whole seconds since t0 (send time)."""
    buckets: dict[int, Second] = {}
    for r in records:
        t = int(r["t_sent"] - t0)
        b = buckets.setdefault(t, Second(t))
        b.attempted += 1
        b.target_rate = r["target_rate"]
        b.phase = r["phase"]
        s = r["status"]
        if s == -1:
            b.skipped += 1
        elif s == 429:
            b.throttled += 1
        elif 200 <= s < 300:
            b.ok += 1
            b.latencies.append(r["latency_ms"])
        else:
            b.other += 1
            if s == 0:
                b.timeouts += 1
    if not buckets:
        return []
    out = []
    for t in range(min(buckets), max(buckets) + 1):
        out.append(buckets.get(t, Second(t)).as_dict())
    return out


def throttle_share(records: list[dict]) -> float:
    sent = [r for r in records if r["status"] != -1]
    if not sent:
        return 0.0
    return sum(1 for r in sent if r["status"] == 429) / len(sent)


# ---------------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------------


def analyze(records: list[dict], step_sec: float | None = None) -> dict:
    """Headline numbers for one endpoint run (one workspace's records).

    - first_429_rate: lowest ramp step with any 429
    - onset_rate: lowest ramp step with >= 5% 429s
    - accepted_ceiling: median accepted (2xx) req/s during hold (~ the bucket refill rate)
    - throttle_after_drop_sec: from load drop to the last 429 (0 = stopped at once)
    - recovery_sec: from load drop to the first second whose calls all succeed
    - retry_after: whether 429s carry Retry-After, and the values seen
    """
    if not records:
        return {"calls": 0}
    t0 = min(r["t_sent"] for r in records)
    ramp: dict[float, list[dict]] = {}
    for r in records:
        if r["phase"] == "ramp":
            ramp.setdefault(r["target_rate"], []).append(r)

    steps = []
    first_429 = onset = None
    for rate in sorted(ramp):
        rs = ramp[rate]
        share = throttle_share(rs)
        sent = [r for r in rs if r["status"] != -1]
        dur = (max(r["t_sent"] for r in rs) - min(r["t_sent"] for r in rs)) or 1.0
        ok_lat = sorted(r["latency_ms"] for r in sent if 200 <= r["status"] < 300)
        steps.append(
            {
                "target_rate": rate,
                "achieved_send_rate": round(len(sent) / dur, 1),
                "accepted_rate": round(sum(1 for r in sent if 200 <= r["status"] < 300) / dur, 1),
                "throttled_share": round(share, 4),
                "other_errors": sum(1 for r in sent if r["status"] not in (429,) and not 200 <= r["status"] < 300),
                "skipped": sum(1 for r in rs if r["status"] == -1),
                "p50_ms": _pct(ok_lat, 0.5),
                "p95_ms": _pct(ok_lat, 0.95),
            }
        )
        if first_429 is None and share > 0:
            first_429 = rate
        if onset is None and share >= 0.05:
            onset = rate

    hold = [r for r in records if r["phase"] == "hold"]
    accepted_ceiling = admitted_ceiling = None
    hold_rate = None
    if hold:
        hold_rate = hold[0]["target_rate"]
        secs = per_second(hold, t0)
        oks = [s["ok"] for s in secs[1:-1]] or [s["ok"] for s in secs]
        accepted_ceiling = statistics.median(oks) if oks else None
        adm = [s["attempted"] - s["throttled"] - s["skipped"] for s in (secs[1:-1] or secs)]
        admitted_ceiling = statistics.median(adm) if adm else None

    burst_sec = None
    if onset is not None:  # how long the onset step ran before its first 429 (bucket depth / excess)
        rs = sorted(ramp[onset], key=lambda r: r["t_sent"])
        f = next((r["t_sent"] for r in rs if r["status"] == 429), None)
        burst_sec = round(f - rs[0]["t_sent"], 2) if f else None

    rec = sorted((r for r in records if r["phase"] == "recovery"), key=lambda r: r["t_sent"])
    throttle_after = recovery = latency_recovery = None
    baseline_p50 = steps[0]["p50_ms"] if steps else None
    if rec:
        drop_t = rec[0]["t_sent"]
        t429 = [r["t_sent"] for r in rec if r["status"] == 429]
        throttle_after = round(max(t429) - drop_t, 2) if t429 else 0.0
        secs = per_second(rec, drop_t)
        for s in secs:  # first second with calls and no 429s / timeouts (legacy answers 400, still counts)
            if s["attempted"] and s["throttled"] == 0 and s["timeouts"] == 0 and s["skipped"] == 0:
                recovery = float(s["t"])
                break
        if baseline_p50:
            # first second after which every second's p50 stays within 2x the low-rate baseline
            ok_secs = [s for s in secs if s["p50_ms"] is not None]
            for i, s in enumerate(ok_secs):
                if all(x["p50_ms"] <= 2 * baseline_p50 for x in ok_secs[i:]):
                    latency_recovery = float(s["t"])
                    break

    throttled = [r for r in records if r["status"] == 429]
    ra_vals = sorted({r["retry_after"] for r in throttled if r.get("retry_after") not in (None, "")})
    codes = sorted({r["error_code"] for r in throttled if r.get("error_code")})
    return {
        "calls": len(records),
        "throttled_calls": len(throttled),
        "first_429_rate": first_429,
        "onset_rate": onset,
        "hold_rate": hold_rate,
        "accepted_ceiling_rps": accepted_ceiling,
        "admitted_ceiling_rps": admitted_ceiling,
        "burst_sec_at_onset": burst_sec,
        "throttle_after_drop_sec": throttle_after,
        "recovery_sec": recovery,
        "baseline_p50_ms": baseline_p50,
        "latency_recovery_sec": latency_recovery,
        "timeouts": sum(1 for r in records if r["status"] == 0),
        "peak_accepted_rps": max((s["accepted_rate"] for s in steps), default=None),
        "retry_after": {
            "share_of_429s_with_header": round(
                sum(1 for r in throttled if r.get("retry_after") not in (None, "")) / len(throttled), 3
            )
            if throttled
            else None,
            "values_seen": ra_vals[:20],
        },
        "error_codes_on_429": codes,
        "steps": steps,
        "step_sec": step_sec,
    }
