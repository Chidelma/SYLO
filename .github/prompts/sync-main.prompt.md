---
description: "Rebase the current branch onto the latest main from origin"
agent: "agent"
tools: [runInTerminal]
---
Safely bring the current branch up to date with the latest `main` from origin.

1. Confirm the current branch with `git branch --show-current`. If already on `main`, just run `git pull` and stop.
2. Stash any uncommitted changes with `git stash push -m "sync-main auto-stash"` and note whether anything was stashed.
3. Fetch the latest: `git fetch origin main`.
4. Rebase the current branch onto `origin/main`: `git rebase origin/main`.
5. If the rebase has conflicts, list the conflicting files and stop — ask the user to resolve them, then run `git rebase --continue`.
6. If a stash was created in step 2, restore it with `git stash pop`.
7. Report: commits rebased, files changed, any stash restored.
