---
description: "Create a GitHub issue for a bug or feature request"
argument-hint: "Describe the bug or feature request"
agent: "agent"
tools: [runInTerminal]
---
Create a GitHub issue. The user's description is provided as the argument.

Determine whether the description sounds like a bug or a feature request, then create the issue with `gh issue create` using the following structure:

**For a bug:**
- Title: "fix: <short description>"
- Body sections: **Describe the bug**, **Steps to reproduce**, **Expected behaviour**, **Actual behaviour**, **Possible cause** (reference relevant lines in `src/index.ts` if applicable), **Environment** (Bun version, TypeScript version, AWS/S3 endpoint details).

**For a feature request:**
- Title: "feat: <short description>"
- Body sections: **Problem**, **Proposed solution**, **Alternatives considered**, **Affected API** (list any methods in `src/types/sylo.d.ts` or `src/types/query.d.ts` that would change).

Apply the appropriate label (`bug` or `enhancement`) via `--label`. Print the issue URL when done.
