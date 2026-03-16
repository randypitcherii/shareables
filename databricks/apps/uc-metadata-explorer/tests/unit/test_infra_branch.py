import pytest

from server.infra_branch import derive_pg_branch_name


@pytest.mark.parametrize(
    ("git_branch", "expected_pg_branch"),
    [
        ("main", "main"),
        ("master", "main"),
        ("feature/uc-metadata-app-lakebase-infra", "uc-metadata-app-lakebase-infra"),
        ("feat/dbx-app-with-pg", "dbx-app-with-pg"),
    ],
)
def test_derive_pg_branch_name_valid(git_branch: str, expected_pg_branch: str) -> None:
    assert derive_pg_branch_name(git_branch) == expected_pg_branch


@pytest.mark.parametrize(
    "git_branch",
    [
        "",  # empty
        "feature/",  # missing suffix
        "Feature/uc-metadata",  # prefix must be lowercase
        "feature/uc_metadata",  # underscores not allowed
        "hotfix/uc-metadata",  # unsupported prefix
        "feature/-starts-with-dash",  # invalid slug start
        "feature/ends-with-dash-",  # invalid slug end
        f"feature/{'a' * 64}",  # suffix exceeds postgres identifier length
    ],
)
def test_derive_pg_branch_name_invalid(git_branch: str) -> None:
    with pytest.raises(ValueError):
        derive_pg_branch_name(git_branch)
