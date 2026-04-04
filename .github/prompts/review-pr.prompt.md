---
description: "Review a pull request by number or URL"
argument-hint: "PR number or URL (e.g. 42)"
agent: "agent"
tools: [runInTerminal]
---
Review the pull request given as an argument (PR number or URL).

1. Fetch the PR details: `gh pr view <arg> --json title,body,headRefName,baseRefName,files`
2. Fetch the diff: `gh pr diff <arg>`
3. Review the changes with focus on:
   - **Correctness** — logic errors, edge cases missed in query parsing ([src/core/parser.ts](src/core/parser.ts)), S3 operations ([src/adapters/s3.ts](src/adapters/s3.ts)), or directory/index management ([src/core/directory.ts](src/core/directory.ts)).
   - **Type safety** — use of `any` instead of `unknown`, missing type guards, incorrect use of types in [src/types/](src/types/).
   - **Tests** — whether new behaviour is covered in [tests/](tests/); flag any missing cases across document, collection, or schema tests.
   - **Public API** — unintended changes to [src/types/sylo.d.ts](src/types/sylo.d.ts) or [src/types/query.d.ts](src/types/query.d.ts).
   - **CI** — whether the workflow files in [.github/workflows/](.github/workflows/) are still valid for the change.
4. Post the review as inline comments using `gh pr review <arg> --comment --body "<feedback>"`.
   Group feedback by file. Prefix each point with **[suggestion]**, **[issue]**, or **[nit]**.
5. Summarise the overall verdict: Approve / Request changes / Comment only.
