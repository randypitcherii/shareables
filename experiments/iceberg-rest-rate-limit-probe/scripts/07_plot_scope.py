"""Plot the loadTable scope test: one workspace alone vs two workspaces on one metastore.

Reads the in-workspace job logs in data/ and writes
  results/loadtable_scope.png   accepted loadTable/s per workspace vs offered rate
  results/loadtable_scope.csv   the values behind the chart (scrubbed aggregates)

    uv run python scripts/07_plot_scope.py [--run-id r2-inws --sp-run-id r2-inws-sp]
"""

import argparse
import csv
import statistics

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from _common import DATA_DIR, RESULTS_DIR  # noqa: E402

# (label, file under data/, colour). Colours pass the dataviz palette check on white.
SERIES = [
    ("one workspace alone", "{run}/load_table_spread-a-job.parquet", "#1f6feb"),
    ("two workspaces, same user", "{run}/load_table_spread-axb-job.parquet", "#d9480f"),
    ("two workspaces, user + service principal", "{sp}/load_table_spread-axb-job.parquet", "#2b8a3e"),
]


def steps(path):
    """Per (workspace, ramp step): offered rate, accepted/s, 429/s, p50 of 2xx."""
    rows = [r for r in pq.read_table(path).to_pylist() if r["phase"] == "ramp"]
    out = {}
    for r in rows:
        out.setdefault((r["workspace"], r["target_rate"]), []).append(r)
    res = []
    for (ws, rate), rs in sorted(out.items()):
        span = max(r["t_sent"] for r in rs) - min(r["t_sent"] for r in rs) or 1
        ok = [r["latency_ms"] for r in rs if 200 <= r["status"] < 300]
        res.append({"workspace": ws, "target_rate": rate, "accepted_rps": round(len(ok) / span, 1),
                    "throttled_rps": round(sum(r["status"] == 429 for r in rs) / span, 1),
                    "p50_ms": round(statistics.median(ok), 1) if ok else None})
    return res


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", default="r2-inws")
    p.add_argument("--sp-run-id", default="r2-inws-sp")
    args = p.parse_args()

    table = []
    for label, pattern, colour in SERIES:
        path = DATA_DIR / pattern.format(run=args.run_id, sp=args.sp_run_id)
        if not path.exists():
            print(f"skip {label}: {path.name} not found")
            continue
        for s in steps(path):
            table.append({"condition": label, **s, "colour": colour})

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2), sharex=True)
    for label, _, colour in SERIES:
        pts = [t for t in table if t["condition"] == label and t["workspace"] == "a"]
        if not pts:
            continue
        x = [t["target_rate"] for t in pts]
        ax1.plot(x, [t["accepted_rps"] for t in pts], "o-", color=colour, label=label)
        ax2.plot(x, [t["p50_ms"] for t in pts], "o-", color=colour, label=label)
        ax1.annotate(f"{pts[-1]['accepted_rps']:.0f}/s", (x[-1], pts[-1]["accepted_rps"]),
                     textcoords="offset points", xytext=(6, -3), color=colour, fontsize=9)
    top = max(t["target_rate"] for t in table)
    ax1.plot([0, top], [0, top], ":", color="#888888", linewidth=1)
    ax1.text(top * 0.55, top * 0.62, "offered = accepted", color="#666666", fontsize=8, rotation=33)
    ax1.set(title="Accepted loadTable calls per workspace", xlabel="offered req/s per workspace",
            ylabel="2xx responses / s (workspace A)", ylim=(0, top * 1.05))
    ax2.set(title="Median latency of successful calls", xlabel="offered req/s per workspace",
            ylabel="p50 ms (workspace A)", ylim=(0, None))
    ax1.legend(loc="upper left", fontsize=8, frameon=False)
    for ax in (ax1, ax2):
        ax.grid(alpha=0.3)
        ax.spines[["top", "right"]].set_visible(False)
    fig.suptitle("loadTable from serverless jobs in the same region: one workspace vs two on one metastore",
                 fontsize=10)
    fig.tight_layout()
    RESULTS_DIR.mkdir(exist_ok=True)
    fig.savefig(RESULTS_DIR / "loadtable_scope.png", dpi=150)
    with open(RESULTS_DIR / "loadtable_scope.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=[k for k in table[0] if k != "colour"], extrasaction="ignore")
        w.writeheader()
        w.writerows(table)
    print(f"wrote {RESULTS_DIR / 'loadtable_scope.png'} and loadtable_scope.csv ({len(table)} rows)")


if __name__ == "__main__":
    main()
