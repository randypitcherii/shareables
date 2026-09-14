"""Row 4 — Which Iceberg type shapes trip federation, per writer path?

Sweeps every shape in SHAPES × every writer in WRITERS. Each cell records the
Glue SD type string for the complex column and the UC read outcome. The point
is a boundary, not a single repro: is it *only* top-level list? Does map<>
survive? Does a list nested in a struct? Does the documented "nested STRUCT
with required fields" limitation bite independently?

Recorded as ONE results row with a per-cell dict, so the README can render a
shape × writer grid from a single key.
"""

from _common import SHAPES, WRITERS, load_config, note, record_result, section, workspace_client
from _matrix_common import identities, probe_uc_read, write_and_inspect


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)
    cells = {}
    for writer in WRITERS:
        for shape in SHAPES:
            section(f"row 04: {shape} via {writer}")
            try:
                written = write_and_inspect(cfg, writer, shape)
            except Exception as e:  # noqa: BLE001 — a writer refusing a shape IS a finding
                note(f"writer refused: {type(e).__name__}: {e}")
                cells[f"{writer}/{shape}"] = {
                    "writer": writer, "shape": shape,
                    "write_error": f"{type(e).__name__}: {str(e)[:800]}",
                    "readable": None,
                }
                continue
            read = probe_uc_read(cfg, written["table"], w)
            complex_cols = {k: v for k, v in written["glue"]["sd_types"].items() if k != "id"}
            cells[f"{writer}/{shape}"] = {
                "writer": writer, "shape": shape, "table": written["table"],
                "sd_complex_types": complex_cols,
                "readable": read["readable"],
                "error_class": read["describe"].get("error_class") or read["select_count"].get("error_class"),
                "error": (read["describe"].get("error") or read["select_count"].get("error") or "")[:600],
            }
    record_result(
        cfg,
        "04_type_shape_sweep",
        {
            "question": "Which Iceberg type shapes fail through Glue federation, "
            "and does the writer path change it?",
            "identity": identities(cfg, w),
            "shapes": list(SHAPES),
            "writers": list(WRITERS),
            "cells": cells,
            "status": "◑",
        },
    )


if __name__ == "__main__":
    main()
