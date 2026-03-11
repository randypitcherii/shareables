import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def pytest_runtest_setup(item):
    """Reset module-level singletons before each test to prevent pollution."""
    try:
        import server.alias_registry as alias_mod
        alias_mod._registry = None
    except (ImportError, AttributeError):
        pass

    try:
        import server.audit as audit_mod
        audit_mod._store = None
    except (ImportError, AttributeError):
        pass

    try:
        import server.routes.permissions as perm_mod
        perm_mod._sp_token = None
        perm_mod._sp_token_expires_at = 0.0
    except (ImportError, AttributeError):
        pass
