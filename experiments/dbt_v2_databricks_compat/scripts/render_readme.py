"""Rewrite the findings matrix in README.md from results/matrix_results.json.

The JSON is the source of truth; the README table is its readable projection,
regenerated between the MATRIX markers so the two can never drift.
"""

from __future__ import annotations

import re

from _common import EXPERIMENT_ROOT, load_results

README = EXPERIMENT_ROOT / "README.md"
MARK = {"pass": "✅", "fail": "❌", "partial": "◑"}
START, END = "<!-- MATRIX:START -->", "<!-- MATRIX:END -->"


def table(data: dict) -> str:
    def order(rid: str) -> tuple[int, int]:
        return (0 if rid.startswith("r") else 1, int(rid[1:]))

    lines = ["| # | Question | Result | Evidence |", "|---|---|---|---|"]
    for rid in sorted(data["rows"], key=order):
        r = data["rows"][rid]
        evidence = r["evidence"].replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {rid} | {r['question']} | {MARK[r['status']]} | {evidence} |")
    return "\n".join(lines)


def main() -> None:
    data = load_results()
    text = README.read_text()
    block = f"{START}\n{table(data)}\n{END}"
    new = re.sub(re.escape(START) + ".*?" + re.escape(END), block, text, flags=re.DOTALL)
    README.write_text(new)
    print(f"rendered {len(data['rows'])} rows into README.md")


if __name__ == "__main__":
    main()
