# FYLO

FYLO is a Bun-native document store that keeps **one canonical file per document** and builds a **collection index file** to make queries fast.

The important mental model is simple:

- document files are the source of truth
- the index file is just an accelerator
- if the index ever gets out of date, FYLO can rebuild it from the documents

FYLO now ships with **one engine**: a filesystem-first storage model designed to work well with AWS S3 Files and other synced filesystem setups.

## Why this design?

We wanted three things:

- low durable storage overhead
- fast application queries
- a system that is still understandable by normal engineers

That is why FYLO does **not** create one tiny durable file per indexed field and does **not** depend on Redis-backed queued writes anymore.

Instead, each collection looks like this:

```text
<root>/<collection>/
  .fylo/
    docs/
      4U/
        4UUB32VGUDW.json
    indexes/
      <collection>.idx.json
    events/
      <collection>.ndjson
```

## Installation

```bash
bun add @delma/fylo
```

## Basic usage

```ts
import Fylo from '@delma/fylo'

const fylo = new Fylo({
    root: '/mnt/fylo'
})

await fylo.createCollection('users')

const id = await fylo.putData('users', {
    name: 'Ada',
    role: 'admin',
    tags: ['engineering', 'platform']
})

const doc = await fylo.getDoc('users', id).once()
console.log(doc[id])
```

## Configuration

FYLO is filesystem-first now.

You can configure the root in one of two ways:

```bash
export FYLO_ROOT=/mnt/fylo
```

Or:

```ts
const fylo = new Fylo({ root: '/mnt/fylo' })
```

If you do not configure a root, FYLO uses a project-local default:

```text
<current working directory>/.fylo-data
```

For compatibility with older `s3-files` experiments, FYLO still accepts `s3FilesRoot` and still reads `FYLO_S3FILES_ROOT` as a fallback.

### Environment variables

| Variable            | Purpose                                                          |
| ------------------- | ---------------------------------------------------------------- |
| `FYLO_ROOT`         | Preferred filesystem root for collections                        |
| `FYLO_S3FILES_ROOT` | Backward-compatible alias for `FYLO_ROOT`                        |
| `SCHEMA_DIR`        | Directory containing JSON validation schemas                     |
| `STRICT`            | When truthy, validate documents with `@delma/chex` before writes |
| `ENCRYPTION_KEY`    | Required when schemas declare `$encrypted` fields                |
| `CIPHER_SALT`       | Recommended unique salt for field encryption key derivation      |

## Security-sensitive behavior

### Encrypted fields

Schemas can declare encrypted fields with a `$encrypted` array. When a collection schema declares encrypted fields, FYLO fails closed unless `ENCRYPTION_KEY` is set and at least 32 characters long.

Encrypted document values are stored with AES-GCM. Exact-match queries on encrypted fields use keyed HMAC blind indexes, so equality and frequency can still be inferred from index tokens, but plaintext field values are not written to document files, index files, or event journals.

If you are upgrading encrypted collections from a version before `2.1.1`, rewrite encrypted documents or otherwise rebuild affected indexes before relying on `$eq` queries for encrypted fields. Older encrypted document bodies can still be read, but old deterministic encrypted index entries do not match the new HMAC blind-index format.

### Bulk imports

`importBulkData()` is intended for trusted JSON or JSONL sources. By default, HTTP(S) imports reject localhost, private, loopback, link-local, and other private-network addresses, and responses are capped at 50 MiB.

You can tighten the import boundary with explicit options:

```ts
await fylo.importBulkData('users', new URL('https://data.example.com/users.json'), {
    limit: 1000,
    maxBytes: 5 * 1024 * 1024,
    allowedHosts: ['data.example.com']
})
```

Only set `allowPrivateNetwork: true` when the import URL is fully trusted by your application.

### Authentication and authorization

FYLO does not authenticate users directly. Your application should verify sessions, JWTs, OAuth tokens, or API keys before calling FYLO.

After your app has an authenticated identity, FYLO can enforce an authorization policy through `fylo.as(authContext)`. Scoped clients fail closed unless a policy is configured:

```ts
import Fylo from '@delma/fylo'

const fylo = new Fylo({
    root: '/mnt/fylo',
    auth: {
        authorize({ auth, action, collection, data }) {
            if (auth.roles?.includes('admin')) return true
            if (collection !== 'posts') return false
            if (action === 'doc:create') {
                return (data as { tenantId?: string }).tenantId === auth.tenantId
            }
            return action === 'doc:read' || action === 'doc:find'
        }
    }
})

const user = await verifyRequest(request)
const db = fylo.as({
    subjectId: user.id,
    tenantId: user.tenantId,
    roles: user.roles
})

const posts = db.findDocs('posts', {
    $ops: [{ tenantId: { $eq: user.tenantId } }]
})
```

The policy receives actions such as `doc:read`, `doc:create`, `doc:update`, `doc:delete`, `bulk:import`, `bulk:export`, `join:execute`, and `sql:execute`. For multi-tenant applications, store tenant or owner fields in each document and deny broad operations like `sql:execute` unless the caller is trusted.

Authorization does not replace filesystem, mount, or object-store permissions. Anyone with direct access to the FYLO root can still access stored files, so keep OS/S3/IAM permissions tight and use encrypted fields for sensitive values.

## Syncing to S3-compatible storage

FYLO does **not** ship its own cloud sync engine.

That is intentional.

The package owns:

- document storage behavior
- query behavior
- index maintenance

You own:

- how that root directory gets synced to AWS S3 Files, S3-compatible storage, or any other file-backed replication layer you trust

That means you can choose the sync tool that matches your infrastructure:

- AWS S3 Files
- `aws s3 sync`
- `rclone`
- storage vendor tooling
- platform-specific replication

If you want FYLO to notify your own S3 client on document writes, you can plug in sync hooks:

```ts
import Fylo from '@delma/fylo'

const fylo = new Fylo({
    root: '/mnt/fylo',
    syncMode: 'await-sync',
    sync: {
        async onWrite(event) {
            const file = Bun.file(event.path)
            await myS3Client.putObject({
                key: `${event.collection}/${event.docId}.json`,
                body: await file.arrayBuffer()
            })
        },
        async onDelete(event) {
            await myS3Client.deleteObject({
                key: `${event.collection}/${event.docId}.json`
            })
        }
    }
})
```

There are two sync modes:

- `await-sync`: FYLO waits for your hook and throws if the remote sync fails
- `fire-and-forget`: FYLO commits locally first and runs your hook in the background

Important detail for junior engineers:

- the filesystem write is still the source of truth
- a sync hook is a replication helper, not the database itself

## CRUD examples

### Create

```ts
const userId = await fylo.putData('users', {
    name: 'Jane Doe',
    age: 29,
    team: 'platform'
})
```

### Read one

```ts
const user = await fylo.getDoc('users', userId).once()
```

### Find many

```ts
const results = {}

for await (const doc of fylo
    .findDocs('users', {
        $ops: [{ age: { $gte: 18 } }]
    })
    .collect()) {
    Object.assign(results, doc)
}
```

### Update one

```ts
const nextId = await fylo.patchDoc('users', {
    [userId]: {
        team: 'core-platform'
    }
})
```

### Delete one

```ts
await fylo.delDoc('users', nextId)
```

## SQL support

FYLO also supports SQL-like commands for app-facing document work:

```ts
await fylo.executeSQL(`
  CREATE TABLE posts
`)

await fylo.executeSQL(`
  INSERT INTO posts VALUES { "title": "Hello", "published": true }
`)

const posts = await fylo.executeSQL(`
  SELECT * FROM posts WHERE published = true
`)
```

## Query behavior

FYLO queries use the collection index file first when they can, then hydrate only the matching documents.

That means:

- exact matches are fast
- range queries are narrowed before document reads
- contains-style queries can use indexed candidates
- final document validation still happens before returning results

This is why FYLO behaves more like an application document store than a data warehouse.

## Realtime behavior

FYLO keeps a filesystem event journal per collection.

That is what powers listeners such as:

```ts
for await (const doc of fylo.findDocs('users', {
    $ops: [{ role: { $eq: 'admin' } }]
})) {
    console.log(doc)
}
```

## What FYLO no longer does

FYLO no longer centers:

- Redis-backed queued writes
- worker-based write draining
- legacy bucket-per-collection S3 storage
- built-in migration commands between old and new engines

If you see older references to those ideas in historic discussions, treat them as previous design stages, not the current product direction.

## Recovery story

This part is important:

- document files are the truth
- index files can be rebuilt

That means FYLO is designed so that the system can recover from index drift without treating the index as a sacred durable database.

## Development

```bash
bun run typecheck
bun run build
bun test
```

## Performance testing

FYLO includes an opt-in scale test for the filesystem engine:

```bash
FYLO_RUN_PERF_TESTS=true bun test tests/integration/s3-files.performance.test.js
```

This is useful when you want to see how index size and query latency behave as collections grow.
