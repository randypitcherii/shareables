from _common import FQ_SCHEMA, must_sql

if __name__ == "__main__":
    must_sql(f"DROP SCHEMA IF EXISTS {FQ_SCHEMA} CASCADE")
    print(
        "OK: experiment schema dropped; uncataloged files remain for explicit storage-owner cleanup"
    )
