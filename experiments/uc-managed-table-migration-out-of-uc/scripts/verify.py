import json

from _common import CATALOG, EXTERNAL_ROOT, GLUE_DATABASE, aws, client, must_sql


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
    caller = aws("sts", "get-caller-identity", "--output", "json")
    if caller.returncode:
        raise SystemExit(f"AWS authentication failed: {caller.stderr}")
    account = json.loads(caller.stdout)["Account"]
    created = aws(
        "glue",
        "create-database",
        "--database-input",
        json.dumps({"Name": GLUE_DATABASE, "Description": "Temporary exit experiment"}),
    )
    if created.returncode and "AlreadyExistsException" not in created.stderr:
        raise SystemExit(f"Glue database setup failed: {created.stderr}")
    print(f"OK: authenticated as {identity[0]} on DBSQL {identity[1]}")
    print(f"OK: catalog {CATALOG!r} and writable external root are reachable")
    print(f"OK: AWS account …{account[-4:]} and independent Glue catalog are reachable")


if __name__ == "__main__":
    main()
