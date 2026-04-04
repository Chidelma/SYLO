---
description: "Create a release branch, publish to npm via CI, then merge to main"
agent: "agent"
tools: [runInTerminal]
---
Create a release branch, publish to npm via CI, then merge to main.

1. Run `bun test` and stop if any tests fail.

2. Determine the new version automatically based on unreleased commits:
   `git log $(git describe --tags --abbrev=0 2>/dev/null || git rev-list --max-parents=0 HEAD)..HEAD --oneline`

   Apply these rules to select the bump type:
   - **major** — any commit with a `!` breaking-change marker (e.g. `feat!:`, `fix!:`) or a `BREAKING CHANGE` footer.
   - **minor** — one or more `feat:` commits and no breaking changes.
   - **patch** — only `fix:`, `chore:`, `docs:`, `refactor:`, `test:`, or `perf:` commits.

   Compute the new version by incrementing the corresponding part of the current `"version"` in [package.json](package.json) and resetting lower parts to zero. Show the chosen version and the reasoning to the user before proceeding.

3. Update `"version"` in [package.json](package.json) to the new version.

4. Fetch the latest main and create a release branch from it:
   ```
   git fetch origin main
   git checkout -b release/<version> origin/main
   ```

5. Stage all changes and commit:
   `git add -A && git commit -m "chore: release v<version>"`

6. Push the branch:
   `git push -u origin release/<version>`

7. Tell the user that the `publish` workflow will now run on GitHub Actions:
   - It verifies the branch name matches `package.json` version.
   - It runs tests, publishes to npm, creates a git tag, and opens a GitHub release.
   - The NPM_TOKEN secret must be set in repo Settings → Secrets → Actions.

8. Once the workflow passes (user confirms), create a PR and merge it to main:
   ```
   gh pr create --title "chore: release v<version>" --body "Release v<version>" --base main --head release/<version>
   gh pr merge --merge --delete-branch
   ```

9. Switch back to main and pull:
   ```
   git checkout main
   git pull
   ```
