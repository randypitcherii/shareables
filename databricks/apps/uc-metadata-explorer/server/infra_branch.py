import re

_BRANCH_PATTERN = re.compile(r"^(feature|feat)/([a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)$")


def derive_pg_branch_name(git_branch: str) -> str:
    branch = git_branch.strip()
    if branch in {"main", "master"}:
        return "main"

    match = _BRANCH_PATTERN.fullmatch(branch)
    if not match:
        raise ValueError(
            "Invalid branch name. Expected 'main'/'master' or 'feature/<kebab-case>' style branch."
        )

    return match.group(2)
