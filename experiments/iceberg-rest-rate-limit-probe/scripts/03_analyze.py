"""Turn raw probe logs (data/<run_id>/*.parquet) into scrubbed, committed results.

Writes:
  results/summary.json            headline numbers per endpoint run, stamped
  results/per_second/<run>.csv    per-second attempted / ok / 429 / latency (graphs)
  results/samples_429.json        distinct 429 (and 5xx) bodies + headers, scrubbed

Hostnames, workspace IDs, UUIDs, emails, catalog and schema names are scrubbed.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import pyarrow.parquet as pq
from _common import DATA_DIR, RESULTS_DIR, load_config, scrub, write_json
from probe_core import analyze, per_second


def load_runs(run_dirs: list[Path]) -> list[tuple[Path, dict, list[dict]]]:
    runs = []
    for d in run_dirs:
        for f in sorted(d.glob("*.parquet")):
            meta = json.loads(f.with_suffix(".meta.json").read_text())
            runs.append((f, meta, pq.read_table(f).to_pylist()))
    return runs


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--runs", default="", help="comma list of run ids under data/ (default: all)")
    args = p.parse_args()
    cfg = load_config()
    extra = (cfg.uc_catalog, cfg.uc_catalog_b, cfg.uc_schema)
    run_dirs = (
        [DATA_DIR / r for r in args.runs.split(",")]
        if args.runs
        else sorted(x for x in DATA_DIR.iterdir() if x.is_dir())
    )
    summary: dict[str, dict] = {}
    samples: dict[str, dict] = {}
    (RESULTS_DIR / "per_second").mkdir(parents=True, exist_ok=True)
    for old in (RESULTS_DIR / "per_second").glob("*.csv"):  # rebuilt from data/ every time
        old.unlink()

    for f, meta, recs in load_runs(run_dirs):
        key = f"{f.parent.name}/{f.stem}"
        t0 = meta["t0"]
        entry = {"stamp": meta["stamp"], "endpoint": meta["endpoint"],
                 "workspaces": meta["workspaces"], "plan": meta["plan"], "by_workspace": {}}
        for ws in meta["workspaces"]:
            rs = [r for r in recs if r["workspace"] == ws]
            entry["by_workspace"][ws] = analyze(rs, meta["plan"]["step_sec"])
            with open(RESULTS_DIR / "per_second" / f"{f.parent.name}__{f.stem}__ws-{ws}.csv",
                      "w", newline="") as fh:
                rows = per_second(rs, t0)
                if rows:
                    w = csv.DictWriter(fh, fieldnames=list(rows[0]))
                    w.writeheader()
                    w.writerows(rows)
        if len(meta["workspaces"]) > 1:
            entry["combined"] = analyze(recs, meta["plan"]["step_sec"])
        summary[key] = entry

        for r in recs:
            if r["status"] == 429 or (r["status"] or 0) >= 500:
                body = scrub(r.get("body"), extra)
                sig = f"{meta['endpoint']}|{r['status']}|{r.get('error_code')}|{(body or '')[:200]}"
                s = samples.setdefault(sig, {"endpoint": meta["endpoint"], "status": r["status"],
                                             "error_code": scrub(r.get("error_code"), extra),
                                             "body": body,
                                             "headers": json.loads(scrub(r.get("headers"), extra) or "{}"),
                                             "count": 0, "runs": set()})
                s["count"] += 1
                s["runs"].add(key)

    for s in samples.values():
        s["runs"] = sorted(s["runs"])
    write_json(RESULTS_DIR / "summary.json", json.loads(scrub(json.dumps(summary), extra)))
    write_json(RESULTS_DIR / "samples_429.json",
               {"samples": sorted(samples.values(), key=lambda s: -s["count"])})
    for key, e in summary.items():
        for ws, a in e["by_workspace"].items():
            print(f"{key:<55} ws={ws} first429={a.get('first_429_rate')} onset={a.get('onset_rate')} "
                  f"ceiling={a.get('accepted_ceiling_rps')} after_drop={a.get('throttle_after_drop_sec')}s "
                  f"recovery={a.get('recovery_sec')}s lat_recovery={a.get('latency_recovery_sec')}s "
                  f"timeouts={a.get('timeouts')} retry_after={a.get('retry_after')}")


if __name__ == "__main__":
    main()
