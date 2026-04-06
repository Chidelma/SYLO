# Fylo

S3-backed NoSQL document store with SQL parsing, Redis-backed write coordination and pub/sub for real-time events, and a CLI.

Documents are stored as **S3 key paths** — not file contents. Each document produces two keys per field: a **data key** (`{ttid}/{field}/{value}`) for full-doc retrieval and an **index key** (`{field}/{value}/{ttid}`) for query lookups. This enables fast reads and filtered queries without a traditional database engine.

Built for **serverless** runtimes (AWS Lambda, Cloudflare Workers) — no persistent in-memory state, lazy connections, minimal cold-start overhead.

Writes are coordinated through Redis before they are flushed to S3. By default the high-level CRUD methods wait for the queued write to be processed so existing code can continue to behave synchronously. If you want fire-and-forget semantics, pass `{ wait: false }` and process queued jobs with a worker or `processQueuedWrites()`.

## Install

```bash
bun add @delma/fylo
```

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `BUCKET_PREFIX` | S3 bucket name prefix |
| `S3_ACCESS_KEY_ID` / `AWS_ACCESS_KEY_ID` | S3 credentials |
| `S3_SECRET_ACCESS_KEY` / `AWS_SECRET_ACCESS_KEY` | S3 credentials |
| `S3_REGION` / `AWS_REGION` | S3 region |
| `S3_ENDPOINT` / `AWS_ENDPOINT` | S3 endpoint (for LocalStack, MinIO, etc.) |
| `REDIS_URL` | Redis connection URL used for pub/sub, document locks, and queued write coordination |
| `FYLO_WRITE_MAX_ATTEMPTS` | Maximum retry attempts before a queued job is dead-lettered |
| `FYLO_WRITE_RETRY_BASE_MS` | Base retry delay used for exponential backoff between recovery attempts |
| `FYLO_WORKER_ID` | Optional stable identifier for a write worker process |
| `FYLO_WORKER_BATCH_SIZE` | Number of queued jobs a worker pulls per read loop |
| `FYLO_WORKER_BLOCK_MS` | Redis stream block time for waiting on new jobs |
| `FYLO_WORKER_RECOVER_ON_START` | Whether the worker reclaims stale pending jobs on startup |
| `FYLO_WORKER_RECOVER_IDLE_MS` | Minimum idle time before a pending job is reclaimed |
| `FYLO_WORKER_STOP_WHEN_IDLE` | Exit the worker loop when no jobs are available |
| `LOGGING` | Enable debug logging |
| `STRICT` | Enable schema validation via CHEX |

## Usage

### CRUD — NoSQL API

```typescript
import Fylo from "@delma/fylo"

const fylo = new Fylo()

// Collections
await Fylo.createCollection("users")

// Create
const _id = await fylo.putData<_user>("users", { name: "John Doe", age: 30 })

// Read one
const user = await Fylo.getDoc<_user>("users", _id).once()

// Read many
for await (const doc of Fylo.findDocs<_user>("users", { $limit: 10 }).collect()) {
    console.log(doc)
}

// Update one
await fylo.patchDoc<_user>("users", { [_id]: { age: 31 } })

// Update many
const updated = await fylo.patchDocs<_user>("users", {
    $where: { $ops: [{ age: { $gte: 30 } }] },
    $set: { age: 31 }
})

// Delete one
await fylo.delDoc("users", _id)

// Delete many
const deleted = await fylo.delDocs<_user>("users", {
    $ops: [{ name: { $like: "%Doe%" } }]
})

// Drop
await Fylo.dropCollection("users")
```

### Queued Writes

```typescript
const fylo = new Fylo()

// Default behavior waits for the queued write to finish.
const _id = await fylo.putData("users", { name: "John Doe" })

// Async mode returns the queued job immediately.
const queued = await fylo.putData("users", { name: "Jane Doe" }, { wait: false })

// Poll status if you need to track progress.
const status = await fylo.getJobStatus(queued.jobId)

// Process pending writes in-process when you are not running a separate worker.
await fylo.processQueuedWrites()
```

When `wait: false` is used, the job is durable in Redis but the document is not visible in S3 until a worker commits it.

Queued jobs that fail are left pending for recovery. Recovered jobs retry up to `FYLO_WRITE_MAX_ATTEMPTS` times before being moved to a dead-letter stream. You can inspect dead letters with `getDeadLetters()` and reclaim stale pending jobs with `processQueuedWrites(count, true)`.

Operational helpers:

- `getQueueStats()` returns current queue, pending, and dead-letter counts
- `getDeadLetters()` lists exhausted jobs
- `replayDeadLetter(streamId)` moves a dead-lettered job back into the main queue

### Worker

Run a dedicated write worker when you want queued writes to be flushed outside the request path:

```bash
bun run worker
```

The worker entrypoint lives at [worker.ts](/Users/iyor/Library/CloudStorage/Dropbox/myProjects/FYLO/src/worker.ts) and continuously drains the Redis stream, recovers stale pending jobs on startup, and respects the retry/dead-letter settings above.

### CRUD — SQL API

```typescript
const fylo = new Fylo()

await fylo.executeSQL(`CREATE TABLE users`)

const _id = await fylo.executeSQL<_user>(`INSERT INTO users (name, age) VALUES ('John Doe', 30)`)

const docs = await fylo.executeSQL<_user>(`SELECT * FROM users LIMIT 10`)

await fylo.executeSQL<_user>(`UPDATE users SET age = 31 WHERE name = 'John Doe'`)

await fylo.executeSQL<_user>(`DELETE FROM users WHERE name LIKE '%Doe%'`)

await fylo.executeSQL(`DROP TABLE users`)
```

### Query Operators

```typescript
// Equality
{ $ops: [{ status: { $eq: "active" } }] }

// Not equal
{ $ops: [{ status: { $ne: "archived" } }] }

// Numeric range
{ $ops: [{ age: { $gte: 18, $lt: 65 } }] }

// Pattern matching
{ $ops: [{ email: { $like: "%@gmail.com" } }] }

// Array contains
{ $ops: [{ tags: { $contains: "urgent" } }] }

// Multiple ops use OR semantics — matches if any op is satisfied
{ $ops: [
    { status: { $eq: "active" } },
    { priority: { $gte: 5 } }
]}
```

### Joins

```typescript
const results = await Fylo.joinDocs<_post, _user>({
    $leftCollection: "posts",
    $rightCollection: "users",
    $mode: "inner",       // "inner" | "left" | "right" | "outer"
    $on: { userId: { $eq: "id" } },
    $select: ["title", "name"],
    $limit: 50
})
```

### Real-Time Streaming

```typescript
// Stream new/updated documents
for await (const doc of Fylo.findDocs<_user>("users")) {
    console.log(doc)
}

// Stream deletions
for await (const _id of Fylo.findDocs<_user>("users").onDelete()) {
    console.log("deleted:", _id)
}

// Watch a single document
for await (const doc of Fylo.getDoc<_user>("users", _id)) {
    console.log(doc)
}
```

### Bulk Import / Export

```typescript
const fylo = new Fylo()

// Import from JSON array or NDJSON URL
const count = await fylo.importBulkData<_user>("users", new URL("https://example.com/users.json"), 1000)

// Export all documents
for await (const doc of Fylo.exportBulkData<_user>("users")) {
    console.log(doc)
}
```

### Rollback

`rollback()` is now a legacy escape hatch.

Fylo still keeps best-effort rollback data for writes performed by the current instance. This is mainly useful for in-process failures and test workflows:

```typescript
const fylo = new Fylo()
await fylo.putData("users", { name: "test" })
await fylo.rollback() // undoes all writes in this instance
```

For queued writes, prefer:

- `getJobStatus()` to inspect an individual write
- `processQueuedWrites(count, true)` to recover stale pending jobs
- `getDeadLetters()` to inspect exhausted jobs
- compensating writes instead of `rollback()` after a commit

`rollback()` may be removed from the main queued-write path in a future major release.

### CLI

```bash
fylo.query "SELECT * FROM users WHERE age > 25 LIMIT 10"
```

### Schema Validation

When `STRICT` is set, documents are validated against CHEX schemas before writes:

```bash
STRICT=true bun run start
```

Schemas are `.d.ts` interface declarations generated by [`@delma/chex`](https://github.com/Chidelma/CHEX).

## Development

```bash
bun test           # Run all tests
bun run build      # Compile TypeScript
bun run typecheck  # Type-check without emitting
bun run lint       # ESLint
```

### Local S3 (LocalStack)

```bash
docker compose up aws
```

This starts LocalStack on `localhost:4566`. Set `S3_ENDPOINT=http://localhost:4566` to route S3 calls locally.

## Security

### What Fylo does NOT provide

Fylo is a low-level storage abstraction. The following must be implemented by the integrating application:

- **Authentication** — Fylo has no concept of users or sessions. Any caller with access to the Fylo instance can read and write any collection.
- **Authorization** — `executeSQL` and all document operations accept any collection name with no permission check. In multi-tenant applications, a caller can access any collection unless the integrator enforces a boundary above Fylo.
- **Rate limiting** — There is no built-in request throttling. An attacker with access to the instance can flood S3 with requests or trigger expensive operations without restriction. Add rate limiting and document-size limits in your service layer.

### Secure configuration

| Concern | Guidance |
|---------|----------|
| AWS credentials | Never commit credentials to version control. Use IAM instance roles or inject via CI secrets. Rotate any credentials that have been exposed. |
| `ENCRYPTION_KEY` | Must be at least 32 characters. Use a high-entropy random value. |
| `CIPHER_SALT` | Set a unique random value per deployment to prevent cross-instance precomputation attacks. |
| `REDIS_URL` | Always set explicitly. Use `rediss://` (TLS) in production with authentication credentials in the URL. |
| Collection names | Must match `^[a-z0-9][a-z0-9\-]*[a-z0-9]$`. Names are validated before any shell or S3 operation. |

### Encrypted fields

Fields listed in `$encrypted` in a collection schema are encrypted with AES-256-CBC. By default a random IV is used per write (non-deterministic). Pass `deterministic: true` to `Cipher.encrypt()` only for fields that require `$eq`/`$ne` queries — deterministic encryption leaks value equality to observers of stored ciphertext.

## License

MIT
