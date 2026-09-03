from _common import MANAGED_CATALOG, must_sql

if __name__ == "__main__":
    must_sql(f"DROP CATALOG IF EXISTS `{MANAGED_CATALOG}` CASCADE")
    print(
        "OK: experiment catalog dropped; object-storage files remain for explicit "
        "storage-owner cleanup"
    )
