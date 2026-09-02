from _common import CATALOG, EXTERNAL_ROOT, client, must_sql


def main() -> None:
    identity = must_sql("SELECT current_user(), current_version().dbsql_version").rows[0]
    must_sql(f"SHOW SCHEMAS IN `{CATALOG}`")
    locations = list(client().external_locations.list())
    matching = [
        location
        for location in locations
        if EXTERNAL_ROOT.startswith((location.url or "").rstrip("/"))
    ]
    if not matching:
        raise SystemExit("EXPERIMENT_EXTERNAL_ROOT is not under a visible UC external location")
    if matching[0].read_only:
        raise SystemExit("The matching UC external location is read-only")
    print(f"OK: authenticated as {identity[0]} on DBSQL {identity[1]}")
    print(f"OK: catalog {CATALOG!r} and writable external root are reachable")


if __name__ == "__main__":
    main()
