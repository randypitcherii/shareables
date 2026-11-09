"""Ramp request rate against one Iceberg REST endpoint until it returns 429, hold, then drop.

    uv run python scripts/01_probe.py --endpoint load_table_spread [--workspaces a,b]

Open-loop: requests go out at the target rate whether or not earlier ones returned,
so a slow server cannot hide a throttle by slowing the client down. In-flight
requests are capped at PROBE_MAX_CONCURRENCY; an attempt over the cap is logged as
`skipped` (status -1) so the attempted rate stays honest.

With --workspaces a,b, both workspaces run the same schedule at the same per-workspace
rate, driven by one controller that watches the combined 429 share. Compare the onset
with the single-workspace run to read the scope of the limit.

Every call is logged (send time, endpoint, status, latency, Retry-After, error code,
429 body + headers) to data/<run_id>/<endpoint>-<mode>.parquet. A live graph is served
at http://127.0.0.1:<port>/ for the duration of the run.

Bounded by PROBE_MAX_RATE, PROBE_MAX_DURATION_SEC and PROBE_MAX_CONCURRENCY.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from pathlib import Path

try:
    HERE = Path(__file__).resolve().parent
except NameError:  # Databricks serverless runs spark_python_task files without __file__
    import sys

    HERE = Path(sys.argv[0]).resolve().parent
    sys.path.insert(0, str(HERE))

import aiohttp  # noqa: E402
import pyarrow as pa
import pyarrow.parquet as pq
from _common import DATA_DIR, host_and_headers, load_config, stamp
from aiohttp import web
from probe_core import ENDPOINTS, ProbePlan, Target, build_request, default_rates, throttle_share

HTML = (HERE / "live_graph.html").read_text()
KEEP_HEADERS = ("retry-after", "content-type", "server", "x-databricks-reason-phrase")
TICK = 0.01
RECORD_SCHEMA = pa.schema([
    ("run_id", pa.string()), ("workspace", pa.string()), ("endpoint", pa.string()),
    ("phase", pa.string()), ("target_rate", pa.float64()), ("t_sent", pa.float64()),
    ("latency_ms", pa.float64()), ("status", pa.int32()), ("retry_after", pa.string()),
    ("error_code", pa.string()), ("body", pa.string()), ("headers", pa.string()),
])


class Live:
    """Incremental per-second aggregates for the live graph (cheap to serve each second)."""

    def __init__(self, t0: float) -> None:
        self.t0 = t0
        self.buckets: dict[str, dict[int, dict]] = {}
        self.state = {"phase": "starting", "target_rate": 0, "calls": 0, "throttled": 0}

    def add(self, ws: str, rec: dict) -> None:
        t = int(rec["t_sent"] - self.t0)
        b = self.buckets.setdefault(ws, {}).setdefault(
            t, {"t": t, "attempted": 0, "ok": 0, "throttled": 0, "other": 0, "lat": [],
                "target_rate": rec["target_rate"]}
        )
        b["attempted"] += 1
        s = rec["status"]
        self.state["calls"] += 1
        if s == 429:
            b["throttled"] += 1
            self.state["throttled"] += 1
        elif 200 <= s < 300:
            b["ok"] += 1
            b["lat"].append(rec["latency_ms"])
        elif s != -1:
            b["other"] += 1

    def series(self, window: int = 900) -> dict:
        out = {}
        for ws, bs in self.buckets.items():
            pts = []
            for t in sorted(bs)[-window:]:
                b = bs[t]
                lat = sorted(b["lat"])
                pts.append({k: b[k] for k in ("t", "attempted", "ok", "throttled", "other", "target_rate")}
                           | {"p50_ms": lat[len(lat) // 2] if lat else None,
                              "p95_ms": lat[int(len(lat) * 0.95)] if lat else None})
            out[ws] = pts
        return {"series": out, "state": self.state}


async def serve_live(live: Live, port: int) -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", lambda _: web.Response(text=HTML, content_type="text/html"))
    app.router.add_get("/api/series", lambda _: web.json_response(live.series()))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", port).start()
    print(f"live graph: http://127.0.0.1:{port}/", flush=True)
    return runner


class Worker:
    """Sends requests for one workspace at the rate the controller sets."""

    def __init__(self, label, endpoint, target, headers, cfg, records, live):
        self.label, self.endpoint, self.target = label, endpoint, target
        self.headers, self.cfg, self.records, self.live = headers, cfg, records, live
        self.inflight = 0
        self.seq = 0
        self.tasks: set[asyncio.Task] = set()

    def _log(self, rec: dict) -> None:
        self.records.append(rec)
        self.live.add(self.label, rec)

    async def _one(self, session: aiohttp.ClientSession, phase: str, rate: float) -> None:
        url, extra = build_request(self.endpoint, self.target, self.seq)
        self.seq += 1
        rec = {"run_id": self.cfg.run_id, "workspace": self.label, "endpoint": self.endpoint,
               "phase": phase, "target_rate": float(rate), "t_sent": time.time(),
               "latency_ms": None, "status": -1, "retry_after": None, "error_code": None,
               "body": None, "headers": None}
        if self.inflight >= self.cfg.max_concurrency:
            self._log(rec)
            return
        self.inflight += 1
        start = time.perf_counter()
        try:
            async with session.get(url, headers=self.headers | extra) as resp:
                body = await resp.read()
                rec["status"] = resp.status
                rec["retry_after"] = resp.headers.get("Retry-After")
                if resp.status >= 400:
                    text = body[:1500].decode("utf-8", "replace")
                    rec["error_code"] = _error_code(text)
                    if resp.status == 429 or resp.status >= 500:
                        rec["body"] = text
                        rec["headers"] = json.dumps(
                            {k: v for k, v in resp.headers.items()
                             if k.lower() in KEEP_HEADERS or "ratelimit" in k.lower()})
        except Exception as e:  # transport error: timeout, reset, DNS
            rec["status"] = 0
            rec["error_code"] = type(e).__name__
        finally:
            self.inflight -= 1
        rec["latency_ms"] = round((time.perf_counter() - start) * 1000, 2)
        self._log(rec)

    async def run_for(self, session, phase: str, rate: float, seconds: float) -> None:
        """Open-loop: launch requests so that rate x elapsed have gone out at every tick."""
        start = time.perf_counter()
        sent = 0
        while (elapsed := time.perf_counter() - start) < seconds:
            due = int(rate * elapsed) + 1 - sent
            for _ in range(max(0, due)):
                t = asyncio.create_task(self._one(session, phase, rate))
                self.tasks.add(t)
                t.add_done_callback(self.tasks.discard)
                sent += 1
            await asyncio.sleep(TICK)

    async def drain(self) -> None:
        if self.tasks:
            await asyncio.wait(list(self.tasks), timeout=35)


def _error_code(text: str) -> str | None:
    try:
        d = json.loads(text)
    except ValueError:
        return text[:60] if text else None
    if isinstance(d, dict):
        if isinstance(d.get("error"), dict):
            e = d["error"]
            return str(e.get("type") or e.get("code") or e.get("message", ""))[:80]
        return str(d.get("error_code") or d.get("error") or "")[:80] or None
    return None


async def fetch_prefix(session, host: str, headers: dict, root: str, catalog: str, required: bool = True) -> str:
    async with session.get(f"{host}{root}/v1/config?warehouse={catalog}", headers=headers) as r:
        if r.status != 200:
            if not required:  # e.g. the legacy endpoint answering 400 "deprecated": probe it anyway
                print(f"note: {root} config returned {r.status}: {(await r.text())[:200]}")
                return f"catalogs/{catalog}"
            raise SystemExit(f"config call failed ({r.status}): {(await r.text())[:300]}")
        d = await r.json()
    return (d.get("overrides") or {}).get("prefix") or (d.get("defaults") or {}).get("prefix") or ""


async def main_async(args) -> None:
    cfg = load_config()
    labels = args.workspaces.split(",")
    plan = ProbePlan(
        rates=[float(x) for x in args.rates.split(",")] if args.rates else default_rates(args.endpoint),
        step_sec=args.step_sec, hold_sec=args.hold_sec, recovery_sec=args.recovery_sec,
        recovery_rate=args.recovery_rate, max_rate=cfg.max_rate,
        max_duration_sec=cfg.max_duration_sec,
    )
    records: list[dict] = []
    t0 = time.time()
    live = Live(t0)
    live.state |= stamp(cfg, ENDPOINTS[args.endpoint][0]) | {"endpoint": args.endpoint}
    runner = None if args.no_live else await serve_live(live, args.port)

    connector = aiohttp.TCPConnector(limit=cfg.max_concurrency + 10, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        workers = []
        for label in labels:
            host, headers = host_and_headers(cfg.profile(label))
            catalog = cfg.catalog(label)
            prefix = await fetch_prefix(session, host, headers, ENDPOINTS["config"][0], catalog)
            legacy_prefix = ""
            if args.endpoint.startswith("legacy_"):
                legacy_prefix = await fetch_prefix(
                    session, host, headers, ENDPOINTS["legacy_config"][0], catalog, required=False)
            target = Target(host=host, catalog=catalog, schema=cfg.uc_schema,
                            tables=tuple(cfg.table_names()), prefix=prefix,
                            legacy_prefix=legacy_prefix)
            workers.append(Worker(label, args.endpoint, target, headers, cfg, records, live))

        async def phase(name: str, rate: float, seconds: float) -> list[dict]:
            live.state |= {"phase": name, "target_rate": rate}
            mark = len(records)
            print(f"[{time.time() - t0:6.1f}s] {name:<8} {rate:>7.1f} req/s x {len(workers)} ws "
                  f"for {seconds:.0f}s", flush=True)
            await asyncio.gather(*(w.run_for(session, name, rate, seconds) for w in workers))
            await asyncio.sleep(0.5)  # let fast responses (429s) land before judging the step
            return records[mark:]

        if args.start_at:  # line up several jobs (one per workspace) for the scope test
            wait = args.start_at - time.time()
            print(f"waiting {wait:.1f}s for --start-at", flush=True)
            await asyncio.sleep(max(0.0, wait))
        t_start = time.time()
        deadline = t_start + cfg.max_duration_sec
        onset = None
        extra_steps = 0
        last_rate = plan.rates[0]
        for rate in plan.rates:
            if time.time() + plan.step_sec > deadline:
                print("duration cap reached; stopping ramp", flush=True)
                break
            last_rate = rate
            step = await phase("ramp", rate, plan.step_sec)
            share = throttle_share(step)
            print(f"           -> 429 share {share:.1%} ({sum(r['status'] == 429 for r in step)} of "
                  f"{len(step)})", flush=True)
            if onset is None and share >= plan.onset_share:
                onset = rate
                live.state["onset_rate"] = rate
            elif onset is not None:
                extra_steps += 1
            if onset is not None and extra_steps >= plan.confirm_steps:
                break

        if onset is not None:
            await phase("hold", plan.hold_rate(onset), plan.hold_sec)
        else:
            print(f"no 429 onset up to {last_rate} req/s; holding at the peak instead", flush=True)
            await phase("hold", last_rate, min(plan.hold_sec, 20))
        await phase("recovery", plan.recovery_rate, plan.recovery_sec)
        for w in workers:
            await w.drain()
    live.state["phase"] = "done"

    out_dir = (Path(args.out_dir) if args.out_dir else DATA_DIR) / cfg.run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    mode = "x".join(labels)
    out = out_dir / f"{args.endpoint}-{mode}.parquet"
    pq.write_table(pa.Table.from_pylist(records, schema=RECORD_SCHEMA), out)
    meta = {"endpoint": args.endpoint, "workspaces": labels, "t0": t0, "onset_rate": onset,
            "plan": plan.__dict__, "stamp": stamp(cfg, ENDPOINTS[args.endpoint][0])}
    out.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2))
    print(f"wrote {len(records)} call records -> {out}", flush=True)
    if args.linger:
        print(f"live graph stays up for {args.linger}s", flush=True)
        await asyncio.sleep(args.linger)
    if runner:
        await runner.cleanup()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--endpoint", required=True, choices=sorted(ENDPOINTS))
    p.add_argument("--workspaces", default="a", help="comma list of workspace labels: a or a,b")
    p.add_argument("--rates", default="", help="override ramp ladder, e.g. 10,20,40")
    p.add_argument("--step-sec", type=float, default=20.0)
    p.add_argument("--hold-sec", type=float, default=45.0)
    p.add_argument("--recovery-sec", type=float, default=60.0)
    p.add_argument("--recovery-rate", type=float, default=2.0)
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--linger", type=float, default=0.0, help="keep the live graph up after the run")
    p.add_argument("--no-live", action="store_true", help="skip the live graph (e.g. inside a job)")
    p.add_argument("--out-dir", default="", help="write logs here instead of data/ (e.g. a UC volume)")
    p.add_argument("--start-at", type=float, default=0.0, help="epoch seconds to start the ramp")
    p.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                   help="set a config variable (jobs have no .env file); repeatable")
    args = p.parse_args()
    for kv in args.set:
        k, _, v = kv.partition("=")
        os.environ[k] = v
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(main_async(args))
        return
    # a notebook/serverless kernel already runs a loop: use a fresh one on a thread
    import threading

    err: list[BaseException] = []
    t = threading.Thread(target=lambda: _run_catching(args, err))
    t.start()
    t.join()
    if err:
        raise err[0]


def _run_catching(args, err: list) -> None:
    try:
        asyncio.run(main_async(args))
    except BaseException as e:  # noqa: BLE001 - re-raised on the caller thread
        err.append(e)


if __name__ == "__main__":
    main()
