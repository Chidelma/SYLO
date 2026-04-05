# FYLO — Project Guidelines

## Overview

FYLO (`@vyckr/fylo`) is an S3-backed NoSQL document store with SQL parsing, Redis pub/sub for real-time events, and a CLI. Documents are stored as S3 key paths — not as file contents — with dual key layouts for data access and indexed queries.

**Assume a serverless deployment model** (e.g., AWS Lambda, Cloudflare Workers). This means:
- No persistent in-memory state across invocations — every request starts cold
- Distributed coordination (e.g., TTID uniqueness) must use external stores like Redis, not in-process caches
- Avoid long-lived connections, background threads, or singleton patterns that assume process longevity
- Keep cold-start overhead minimal — lazy initialization over eager setup

## Architecture

### Key Storage Format

- **Data keys**: `{ttid}/{field}/{value}` — keyed by document ID for full-doc retrieval
- **Index keys**: `{field}/{value}/{ttid}` — keyed by field for query lookups
- Nested objects flatten to path segments: `address/city/Toronto`
- Forward slashes in values are escaped with an ASCII substitute

### Core Modules

| Module | Responsibility |
|--------|---------------|
| `src/index.ts` | Main `Fylo` class — CRUD, SQL execution, joins, bulk ops |
| `src/core/parser.ts` | SQL lexer/parser — tokenizes SQL into query objects |
| `src/core/query.ts` | Converts `$ops` into glob patterns for S3 key matching |
| `src/core/walker.ts` | S3 key traversal, document data retrieval, Redis event streaming |
| `src/core/directory.ts` | Key extraction, reconstruction, rollback tracking |
| `src/core/format.ts` | Console formatting for query output |
| `src/adapters/s3.ts` | S3 adapter (Bun S3Client) |
| `src/adapters/redis.ts` | Redis adapter (Bun RedisClient) |
| `src/cli/index.ts` | CLI entry point (`fylo.query`) |

### Folder Structure

```
src/
  index.ts                # Public API — main Fylo class
  adapters/               # I/O boundary abstractions (S3, Redis)
  core/                   # Internal domain logic (parser, query, walker, directory)
  cli/                    # CLI entry point
  types/                  # Type declarations (.d.ts only — separate from implementation)
tests/
  data.ts                 # Shared test data URLs
  index.ts                # Test barrel
  mocks/                  # Mock adapters (S3, Redis) for testing
  schemas/                # CHEX-generated test schemas (.d.ts + .json)
  integration/            # End-to-end tests (CRUD, operators, joins, edge cases)
```

### Dependencies

- **`@vyckr/ttid`** — Time-based unique ID system. `TTID.generate()` creates new IDs; `TTID.generate(existingId)` creates a versioned ID sharing the same creation-time prefix.
- **`@vyckr/chex`** — Schema validation. Generates `interface` declarations in `.d.ts` files. Generic constraints must use `Record<string, any>` (not `Record<string, unknown>`) to accept these interfaces.
- **`Bun.Glob`** — Pattern matching for queries. Does NOT support negation extglob `!(pattern)`. Operators like `$ne`, `$gt`, `$lt` use broad globs with post-filtering instead.

## Engineering Standards

- **SOLID principles**: Single responsibility per class/method, depend on abstractions (e.g., S3/Redis adapters), open for extension without modifying core logic
- **Clean code**: Descriptive naming, small focused functions, no dead code or commented-out blocks, DRY without premature abstraction
- **Test discipline**: When changing `src/` code, update or add corresponding tests in `tests/` — never leave tests stale after a behaviour change
- **Error handling**: Fail fast with meaningful errors at system boundaries; use rollback mechanisms for partial writes
- **No magic values**: Use constants or environment variables; avoid hardcoded strings/numbers in logic
- **Type safety**: Leverage TypeScript's type system fully — avoid `any` in implementation code, prefer narrow types, and validate at I/O boundaries

## Code Style

- **Runtime**: Bun (ESNext target, ES modules)
- **Strict TypeScript**: `strict: true`, `noImplicitReturns`, `isolatedModules`
- **ESLint** enforces `@typescript-eslint/no-explicit-any` in `src/` and `tests/` — use it only in type declarations (`.d.ts`)
- **No default exports** except the main `Fylo` class
- Prefer `class` with `static` methods for modules (no standalone functions)
- Use `_ttid` branded type for document IDs — never plain `string`
- Prefix internal/test type names with underscore: `_post`, `_album`, `_storeQuery`
- Type declarations live in `src/types/*.d.ts` — keep separate from implementation

## Build & Test

```bash
bun test           # Run all tests
bun run build      # Compile TypeScript
bun run typecheck  # Type-check without emitting
bun run lint       # ESLint
```

- Tests use `bun:test` — `describe`, `test`, `expect`, `mock`, `beforeAll`, `afterAll`
- S3 and Redis are mocked via `mock.module()` in every test file using `tests/mocks/s3.ts` and `tests/mocks/redis.ts`
- Test schemas live in `tests/schemas/*.d.ts` as global `interface` declarations (generated by CHEX)
- Test data URLs are centralized in `tests/data.ts`

## Conventions

- Collection names may contain hyphens (e.g., `ec-test`, `jm-album`) — the parser supports this
- Nested field access in SQL uses dot notation (`address.city`) which the parser converts to slash-separated paths (`address/city`)
- `putData` creates documents; `patchDoc` updates them (deletes old keys, writes new ones)
- `getDocData` retrieves keys for a specific TTID — filters by exact ID, not just prefix
- Query `$ops` use OR semantics — a document matches if it satisfies at least one operator
- `$limit` on queries without `$ops` uses S3 `maxKeys`; with `$ops` it post-filters after glob matching

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `BUCKET_PREFIX` | S3 bucket name prefix |
| `S3_ACCESS_KEY_ID` / `AWS_ACCESS_KEY_ID` | S3 credentials |
| `S3_SECRET_ACCESS_KEY` / `AWS_SECRET_ACCESS_KEY` | S3 credentials |
| `S3_REGION` / `AWS_REGION` | S3 region |
| `S3_ENDPOINT` / `AWS_ENDPOINT` | S3 endpoint (for compatible stores) |
| `REDIS_URL` | Redis connection URL |
| `LOGGING` | Enable debug logging |
| `STRICT` | Enable schema validation via CHEX |
