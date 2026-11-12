"""Row 03: Structured Streaming with each format as a sink (writeStream.toTable) and as a
plain source (readStream.table, no CDF). Each stream runs twice with availableNow; between
runs two rows are appended to the source, so a pass requires 3 then 5 sink rows (incremental
progress, not a re-snapshot)."""

from _common import short, spark_raw, write_result

STREAMS = {
    "delta": {
        "sink (Delta source -> Delta sink, control)": "control_delta_to_delta",
    },
    "iceberg": {
        "sink, default writer (Delta source -> Iceberg)": "sink_iceberg_default_writer",
        "sink, .format('iceberg') (Delta source -> Iceberg)": "sink_iceberg_format_iceberg",
        "plain source (Iceberg -> Delta)": "source_iceberg_plain_to_delta",
        "source + sink, .format('iceberg') (Iceberg -> Iceberg)": "iceberg_to_iceberg",
    },
}


def judge(c, key):
    first, inc = c.get(f"stream_{key}_first"), c.get(f"stream_{key}_incremental")
    if not first["ok"]:
        return False, f"failed: {short(first)}"
    n1 = len(first["value"]["sink_rows"])
    if not inc or not inc["ok"]:
        return False, f"first run {n1} rows; incremental run failed: {short(inc or {})}"
    n2 = len(inc["value"]["sink_rows"])
    return (n1, n2) == (3, 5), f"{n1} -> {n2} rows"


def main() -> None:
    raw = spark_raw()
    c = raw["cells"]
    evidence = {"dbr": raw.get("dbr")}
    cells = {}
    for fmt, streams in STREAMS.items():
        results = {label: judge(c, key) for label, key in streams.items()}
        evidence[fmt] = {label: {k: c.get(f"stream_{key}_{k}") for k in ("first", "incremental")}
                         for label, key in streams.items()}
        n_ok = sum(ok for ok, _ in results.values())
        status = "pass" if n_ok == len(results) else ("fail" if n_ok == 0 else "partial")
        cells[fmt] = {
            "status": status,
            "finding": "; ".join(f"{label}: {'✅' if ok else '❌'} {note}" for label, (ok, note) in results.items()),
        }
    write_result(
        "03_structured_streaming",
        question="Does each format work as a Structured Streaming sink and as a plain streaming source?",
        cells=cells,
        evidence=evidence,
    )


if __name__ == "__main__":
    main()
