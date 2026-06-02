# Agent Instructions

This project uses **GitHub Issues** for task tracking. Use `gh issue` commands
or the repo Issues tab to find, create, claim, and close work.

## Quick Reference

```bash
gh issue list --state open                       # Find available work
gh issue view <number>                           # View issue details
gh issue edit <number> --add-assignee @me        # Claim work
gh issue create --title "..." --body "..."       # Create follow-up work
gh issue close <number>                          # Complete work
```

## Issue Tracking

**IMPORTANT**: Use GitHub Issues for all durable task tracking.

- Create issues for follow-up work that should survive the current session.
- Link related work with issue references such as `Discovered from #123`.
- Use labels for type and priority, such as `bug`, `feature`, `task`, `priority: high`, or `priority: backlog`.
- Use `gh issue list --json number,title,labels,assignees,state` for programmatic issue queries.
- Do NOT use repo-local issue trackers or duplicate durable task systems.

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create GitHub Issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished issues, update in-progress issues
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
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

**Creating PRs:**
```bash
gh pr create --title "..." --body "..." --base main
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
