# Changelog

## 3.0.0 — 2026-04-24

Major release. All backwards-compatibility surface removed; codebase
reorganised into a domain-driven folder structure. There are no new
runtime features beyond what shipped in the 2.x hardening pass — this
release is greenfield-only and intended for fresh deployments.

### Breaking Changes

#### Constructor / root options

- `s3FilesRoot` and `filesystemRoot` constructor options are removed.
  Use `root` instead.
- `FYLO_S3FILES_ROOT` and `FYLO_FILESYSTEM_ROOT` environment variables
  are removed. Use `FYLO_ROOT` instead.

#### Encryption

- The AES-CBC legacy read path (`Cipher.legacyCbcKey`) is removed.
  Documents encrypted before v2.1.1 (CBC mode) cannot be decrypted by
  this release. No migration path is provided; this is a greenfield
  release.

#### Write-operation options

- The `{ wait: false }` option on `putData`, `patchDoc`, and `delDoc`
  is removed entirely. Passing `options` to these methods previously
  threw a descriptive error; callers now receive a standard
  `TypeError: ... is not a function` if they call the old three-argument
  form through a stale binding.
- `rollback()` is removed from both `Fylo` and `ScopedFylo`. It was a
  no-op placeholder since the async-queue era was dropped.

#### Queue / dead-letter tombstone stubs

The nine stub methods that were kept solely to emit descriptive errors
are gone: `queuePutData`, `queuePatchDoc`, `queueDelDoc`,
`processQueuedWrites`, `getJobStatus`, `getDocStatus`, `getDeadLetters`,
`getQueueStats`, `replayDeadLetter`.

#### SSRF guard moved off the class

`Fylo.normalizeImportOptions`, `Fylo.assertImportUrlAllowed`,
`Fylo.isPrivateIPv4`, `Fylo.expandIPv6`, `Fylo.isPrivateAddress`,
`Fylo.hostAllowed`, and `Fylo.DEFAULT_IMPORT_MAX_BYTES` are no longer
static members of the `Fylo` class. They are now named exports of
`src/security/import-guard.js` (or `dist/security/import-guard.js`).

#### Source / import paths (dist consumers)

The source tree has been reorganised from a flat-ish layout to a
domain-driven structure. If you imported from internal paths, update:

| Old path | New path |
|---|---|
| `src/adapters/cipher.js` | `src/security/cipher.js` |
| `src/sync.js` (events) | `src/observability/events.js` |
| `src/sync.js` (sync/worm) | `src/replication/sync.js` |
| `src/engines/filesystem.js` | `src/storage/engine.js` |
| `src/engines/filesystem/durable.js` | `src/storage/durable.js` |
| `src/engines/filesystem/fs-lock.js` | `src/storage/fs-lock.js` |
| `src/engines/filesystem/storage.js` | `src/storage/primitives.js` |
| `src/engines/filesystem/types.js` | `src/storage/types.js` |
| `src/engines/types.js` | `src/storage/types.js` |
| `src/core/format.js` | `src/cli/format.js` |
| `src/core/directory.js` | `src/storage/index-keys.js` |

### Security

- **SSRF guard with reason codes.** `importBulkData` classifies and
  reports rejected URLs as `protocol`, `host`, `private-network`, or
  `redirect`. Private/loopback/link-local IPv4 + IPv6 ranges are blocked
  by default; off-host redirects are rejected after the first hop.
- **`CIPHER_SALT` is fail-closed.** When a collection schema requires
  encryption (`$encrypted` fields) and `CIPHER_SALT` is missing, FYLO
  refuses to configure the cipher rather than silently deriving a
  default. Deployments must set `CIPHER_SALT` explicitly before any
  encrypted write or read.

### Durability

- **Atomic lock create.** `tryAcquireFileLock` uses `link()` from a
  pre-populated temp file rather than `open(wx)` + `write`, closing a
  window where a concurrent reader could observe an empty lock.
- **Stale-lock takeover is observable.** Reclaimed stale collection or
  document write locks emit a `lock.takeover` event with the previous
  owner.
- **Heartbeat on collection write locks.** Long-running operations
  (`rebuildCollection`, bulk writes) refresh the lock timestamp every
  `ttlMs/3` while held, so legitimate work past the TTL is no longer
  misclassified as stale and taken over by another process.
- **Write-lane leak fixed.** `withCollectionWriteLock` now releases its
  in-process write lane even if the underlying collection-lock
  acquisition throws (e.g. on lock-wait timeout), preventing the lane
  from getting permanently stuck pending.

### Performance

- **Index writes amortized.** Collection index updates are batched
  within a single write lane, eliminating O(n²) behavior on bulk
  imports and rebuilds.

### Observability

- **`onEvent` hook on the `Fylo` constructor.** Receives a discriminated
  union of structured events:
  - `import.blocked` — `{ reason, url, detail? }`
  - `cipher.configured` — `{ collection }`
  - `index.rebuilt` — `{ collection, docsScanned, indexedDocs, worm }`
  - `lock.takeover` — `{ lockPath, newOwner, previousOwner? }`

  Throwing handlers are caught and logged to `console.error`; they do
  not break the underlying operation.

## 2.3.0

(Prior release. See git history.)
