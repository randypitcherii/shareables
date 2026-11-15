"""Delete the test caller SP and its local state. Run after `databricks bundle destroy`."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import STATE_FILE, client, load_test_caller, ok, section  # noqa: E402


def main() -> int:
    section("teardown: test caller SP")
    if not STATE_FILE.exists():
        ok("nothing to remove")
        return 0
    tc = load_test_caller()
    client("owner").service_principals.delete(id=tc["id"])
    STATE_FILE.unlink()
    ok("deleted the SP and .state/test_caller.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
