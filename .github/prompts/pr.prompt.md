---
description: "Create a pull request for the current branch into main"
agent: "agent"
tools: [runInTerminal]
---
Create a pull request for the current branch into `main`.

1. Run `git status` and stop if there are uncommitted changes — ask the user to commit or stash them first.
2. Run `git log main..HEAD --oneline` to list commits on this branch.
3. Run `git diff main...HEAD` to understand all changes.
4. Push the branch to origin if it has no upstream: `git push -u origin HEAD`.
5. Create the PR with `gh pr create` using:
   - A concise title (≤ 70 chars) derived from the branch name and commits.
   - A body with three sections:
     - **Summary** — bullet list of what changed and why.
     - **Test plan** — checklist of how to verify the changes (reference `bun test` where relevant).
     - **Breaking changes** — any changes to public API in [src/types/sylo.d.ts](src/types/sylo.d.ts) or [src/types/query.d.ts](src/types/query.d.ts); write "None" if there are none.
6. Print the PR URL.
