# Agent Instructions

This project uses **bd** (beads) for issue tracking. Run `bd onboard` to get started.

## Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --status in_progress  # Claim work
bd close <id>         # Complete work
bd sync               # Sync with git
```

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd sync
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

## PR Workflow

- PRs are **squash-merged** (no regular merges or rebases)
- Always get **user approval** before opening a PR
- Once approved to open a PR, you are also authorized to squash-merge it
- After merging, pull latest main and clean up local feature branches

**Creating PRs** — use the GitHub API directly (`gh pr create` fails due to EMU restrictions):
```bash
gh api repos/{owner}/{repo}/pulls -f title="..." -f head="..." -f base="main" -f body="..."
```

**Merging PRs:**
```bash
gh pr merge <number> --squash --delete-branch
```

**Post-merge cleanup:**
```bash
git checkout main
git pull --rebase
git branch -d <feature-branch>
```

