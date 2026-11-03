"""Drop the experiment catalog (CASCADE). Managed files are purged by UC after its retention."""

from _common import CATALOG, must_sql


def main() -> None:
    must_sql(f"DROP CATALOG IF EXISTS `{CATALOG}` CASCADE")
    print(f"OK: dropped catalog {CATALOG}")


if __name__ == "__main__":
    main()
