# /// script
# requires-python = ">=3.11"
# dependencies = ["tzdata", "pycountry"]
# ///
"""Regenerate the reference seeds for the seed-validation pattern.

    uv run scripts/generate_pattern_seeds.py      # or: make patterns-seeds

Sources are pinned by the package versions uv resolves (tzdata for the IANA tz
database, pycountry for ISO 3166-1), so the seeds are reproducible and a
refresh shows up as a reviewable CSV diff.
"""

import csv
from importlib import resources
from pathlib import Path

import pycountry
import tzdata  # noqa: F401 -- the data package, read below

SEEDS = Path(__file__).resolve().parents[1] / "seeds" / "patterns"
# tz database areas that are not real Area/Location names
EXCLUDED_PREFIXES = ("Etc/", "SystemV/", "posix/", "right/")


def iana_timezones() -> list[tuple[str, str]]:
    zones = resources.files("tzdata").joinpath("zones").read_text().split()
    names = sorted(
        z
        for z in zones
        if z == "UTC" or ("/" in z and not z.startswith(EXCLUDED_PREFIXES))
    )
    return [(name, name.split("/")[0]) for name in names]


def iso_region_codes() -> list[tuple[str, str]]:
    return sorted((c.alpha_2, c.name) for c in pycountry.countries)


def write(path: Path, header: tuple[str, str], rows: list[tuple[str, str]]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    print(f"wrote {len(rows)} rows to {path.relative_to(SEEDS.parents[1])}")


if __name__ == "__main__":
    write(SEEDS / "ref_iana_timezones.csv", ("timezone", "area"), iana_timezones())
    write(
        SEEDS / "ref_iso_region_codes.csv",
        ("region_code", "region_name"),
        iso_region_codes(),
    )
