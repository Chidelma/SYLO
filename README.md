# FYLO

FYLO is a Bun-native document store that keeps **one canonical file per document** and builds a **collection index file** to make queries fast.

The important mental model is simple:

- document files are the source of truth
- the index file is just an accelerator
- if the index ever gets out of date, FYLO can rebuild it from the documents

FYLO now ships with **one engine**: a filesystem storage model. Cloud replication, object-store syncing, and mounted storage are deployment concerns around that engine, not separate FYLO engines.

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

When WORM mode is enabled, FYLO also keeps lineage metadata:

```text
<root>/<collection>/
  .fylo/
    docs/
    versions/
      <version-id>.meta.json
    heads/
      <lineage-id>.json
    indexes/
      <collection>.idx.json
    events/
      <collection>.ndjson
```

## Installation

```bash
bun add @d31ma/fylo
```

If you install the package from GitHub Packages, configure your `.npmrc` first:

```text
@d31ma:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${NODE_AUTH_TOKEN}
```

FYLO publishes three CLI entrypoints:

- `fylo.query` for SQL and admin commands
- `fylo.admin` as an explicit admin alias for operational flows
- `fylo.exec` for the language-agnostic JSON machine interface

## Basic usage

```ts
import Fylo from '@d31ma/fylo'

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

Use the `root` constructor option or `FYLO_ROOT` environment variable to
configure where collections are stored. The legacy `filesystemRoot`,
`s3FilesRoot`, `FYLO_FILESYSTEM_ROOT`, and `FYLO_S3FILES_ROOT` options
and environment variables were removed in v3.0.0.

### Environment variables

| Variable         | Purpose                                                          |
| ---------------- | ---------------------------------------------------------------- |
| `FYLO_ROOT`      | Filesystem root for collections                                  |
| `SCHEMA_DIR`     | Directory containing JSON validation schemas                     |
| `STRICT`         | When truthy, validate documents with `@d31ma/chex` before writes |
| `ENCRYPTION_KEY` | Required when schemas declare `$encrypted` fields                |
| `CIPHER_SALT`    | Recommended unique salt for field encryption key derivation      |

## Security-sensitive behavior

### Encrypted fields

Schemas can declare encrypted fields with a `$encrypted` array. When a collection schema declares encrypted fields, FYLO fails closed unless `ENCRYPTION_KEY` is set and at least 32 characters long.

Encrypted document values are stored with AES-GCM. Exact-match queries on encrypted fields use keyed HMAC blind indexes, so equality and frequency can still be inferred from index tokens, but plaintext field values are not written to document files, index files, or event journals.

Documents encrypted with AES-GCM (v2.1.1 and later) are fully
supported. The legacy AES-CBC read path was removed in v3.0.0; documents
written before v2.1.1 cannot be decrypted by this release.

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
import Fylo from '@d31ma/fylo'

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

If you expose FYLO rebuild tooling in a multi-tenant app, treat it as an administrative action. The policy action name for that path is `collection:rebuild`.

## WORM mode

WORM mode makes FYLO append-only at the logical document level.

- every update writes a new immutable document version
- FYLO keeps one logical head per lineage for normal queries
- `getDoc(versionId)` can still read a specific retained version directly
- `getLatest()` resolves the current head for a version or lineage ID
- `getHistory()` walks the retained version chain from newest to oldest

Timestamp note:

- `createdAt` and `updatedAt` come from the TTID, which is also the document filename
- FYLO does not use filesystem metadata such as `mtime` for query semantics
- that keeps timestamp behavior stable across sync tools, copies, restores, and object-store rewrites

Enable it like this:

```ts
const fylo = new Fylo({
    root: '/mnt/fylo',
    worm: {
        mode: 'append-only',
        deletePolicy: 'tombstone'
    }
})
```

There are two WORM delete policies:

- `reject`: `delDoc()` and `delDocs()` throw
- `tombstone`: FYLO removes the head from active query results while preserving retained versions and history

Example:

```ts
const firstId = await fylo.putData('posts', { title: 'v1' })
const secondId = await fylo.patchDoc('posts', {
    [firstId]: { title: 'v2' }
})

const latest = await fylo.getLatest('posts', firstId)
const history = await fylo.getHistory('posts', secondId)
```

## Syncing to external storage

FYLO does **not** ship its own cloud sync engine.

That is intentional.

The package owns:

- document storage behavior
- query behavior
- index maintenance

You own:

- how that root directory gets synced to mounted storage, S3-compatible storage, or any other file-backed replication layer you trust

That means you can choose the sync tool that matches your infrastructure:

- mounted filesystems
- `aws s3 sync`
- `rclone`
- storage vendor tooling
- platform-specific replication

If you want FYLO to notify your own storage client on document writes, you can plug in sync hooks:

```ts
import Fylo from '@d31ma/fylo'

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

In WORM mode, sync hooks include extra lineage metadata under `event.worm`:

```ts
sync: {
    async onWrite(event) {
        if (event.worm?.headOperation === 'advance') {
            console.log('new version', event.docId, 'advanced lineage', event.worm.lineageId)
        }
    },
    async onDelete(event) {
        if (event.worm?.deleteMode === 'tombstone') {
            console.log('lineage tombstoned at', event.worm.headPath)
        }
    }
}
```

Important sync detail:

- non-WORM patches still emit a delete for the old document file and a write for the new file
- WORM patches emit only the new version write plus `event.worm.headOperation = 'advance'`
- WORM tombstones emit a logical delete against the head path, with the retained version file exposed as `event.worm.versionPath`

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

For timestamp filters such as `$created` and `$updated`, FYLO derives the values from the TTID rather than the underlying file metadata.

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

## Known limitations

These are deliberate boundaries of the current design. Read them before
adopting FYLO in scenarios where they matter.

- **Filesystem-only.** There is one engine, and it writes to a local
  filesystem path. Replication to S3, GCS, or any remote target is the
  consumer's responsibility — see "Syncing to external storage". FYLO
  does not retry, queue, or reconcile remote writes.
- **Advisory locking, not fencing.** Cross-process write coordination
  uses lockfiles with a TTL (default 30s). Holders that exceed the TTL
  can have their lock reclaimed by another process — long operations
  on the same collection from two writers can race. Lockfiles also
  assume the underlying filesystem honors atomic `link()`; networked
  filesystems without that guarantee are not supported.
- **Single-process index cache.** The collection index file is the
  shared on-disk representation, but an in-memory cache lives per
  process. External processes that mutate documents directly on disk
  will not be visible to a running FYLO process until rebuild or
  restart. Treat the FYLO API as the only writer.
- **Logical, not physical, WORM.** WORM mode is enforced by FYLO's
  write paths. Anyone with direct filesystem access to the root can
  still rewrite or delete document files — pair WORM with OS-level
  permissions or an immutable-storage mount when you need true
  immutability.
- **Frequency leaks on encrypted equality search.** Encrypted fields
  are stored with AES-GCM, but `$eq` queries use deterministic HMAC
  blind indexes. An attacker with read access to the index file can
  count repetitions and infer that two documents share a value, even
  without learning the value itself.
- **Process-global cipher.** `ENCRYPTION_KEY` and `CIPHER_SALT` are
  resolved from the environment once per process and shared across all
  collections that declare `$encrypted` fields. There is no per-tenant
  or per-collection key rotation built in; rotation is an external
  rewrite operation.
- **Bulk import is for trusted sources.** The default SSRF guard blocks
  private and loopback addresses and caps response size at 50 MiB, but
  it does not parse, sandbox, or rate-limit the source. Treat
  `importBulkData` as administrative and call it from server-side code
  with vetted inputs — never from a request handler that forwards
  user-provided URLs.
- **No multi-document or multi-collection transactions.** Writes are
  serialized per collection. There is no atomic commit across
  collections, no two-phase commit with a sync hook, and no rollback
  semantics beyond the local filesystem write.
- **TTID-derived timestamps.** `createdAt` and `updatedAt` come from
  the document's TTID, not filesystem `mtime`. This is stable across
  copies and rebuilds, but it also means external tools that rewrite
  document files cannot change the timestamps FYLO reports.

## Recovery story

This part is important:

- document files are the truth
- index files can be rebuilt

That means FYLO is designed so that the system can recover from index drift without treating the index as a sacred durable database.

Use `rebuildCollection()` when:

- the index file drifts from document reality
- WORM head files or version metadata need to be normalized
- you want to repair retained lineage metadata after a manual filesystem recovery

Rebuild timestamp rule:

- FYLO rebuilds timestamp behavior from TTIDs
- it does not trust filesystem metadata like `mtime` as the source of `createdAt` or `updatedAt`

```ts
const result = await fylo.rebuildCollection('posts')
console.log(result)
```

`rebuildCollection()`:

- scans retained document files
- rebuilds the collection index from scratch
- in WORM mode, rewrites head files and version metadata
- preserves tombstoned lineages by carrying forward retained `deletedAt` metadata

Typical result shape:

```ts
{
    collection: 'posts',
    worm: true,
    docsScanned: 42,
    indexedDocs: 30,
    headsRebuilt: 30,
    versionMetasRebuilt: 42,
    staleHeadsRemoved: 1,
    staleVersionMetasRemoved: 0
}
```

You can also run rebuilds from the bundled CLI:

```bash
fylo.admin rebuild posts --root /mnt/fylo --json
```

Or:

```bash
fylo.query rebuild posts --root /mnt/fylo
```

`fylo.query` remains backward-compatible with raw SQL input:

```bash
fylo.query "SELECT * FROM posts WHERE published = true"
fylo.query sql "SELECT * FROM posts WHERE published = true"
```

Admin commands also support direct collection inspection and version navigation:

```bash
fylo.admin inspect posts --root /mnt/fylo --json
fylo.admin get posts 4UUB32VGUDW --root /mnt/fylo --json
fylo.admin latest posts 4UUB32VGUDW --root /mnt/fylo --worm --json
fylo.admin latest posts 4UUB32VGUDW --root /mnt/fylo --worm --id-only
fylo.admin history posts 4UUB32VGUDW --root /mnt/fylo --worm --json
fylo.query sql "SELECT * FROM posts" --page-size 25 --align left
fylo.query sql "SELECT * FROM posts" --no-pager
```

For non-JavaScript callers, FYLO also exposes a stable JSON machine interface:

```bash
fylo.exec exec --request '{"op":"inspectCollection","root":"/mnt/fylo","collection":"posts"}'
cat request.json | fylo.exec exec --request -
fylo.exec exec --request @request.json
```

Machine responses are always JSON and use a small protocol envelope:

```json
{
    "protocolVersion": 1,
    "ok": true,
    "op": "inspectCollection",
    "requestId": "optional-correlation-id",
    "durationMs": 4,
    "result": {
        "collection": "posts",
        "exists": true
    }
}
```

This is the recommended boundary when you want to compile FYLO into a Bun executable and call it from Python, Go, Rust, Java, or shell automation:

```bash
bun build --compile ./src/cli/index.js --outfile ./dist/bin/fylo
./dist/bin/fylo exec --request @request.json
```

Example request payload:

```json
{
    "requestId": "get-latest-1",
    "op": "getLatest",
    "root": "/mnt/fylo",
    "collection": "posts",
    "id": "4UUB32VGUDW"
}
```

Language examples all use the same executable contract: write one JSON request to stdin and read one JSON response from stdout.

Python:

```python
import json
import subprocess

request = {
    "requestId": "inspect-posts-1",
    "op": "inspectCollection",
    "root": "/mnt/fylo",
    "collection": "posts",
}

proc = subprocess.run(
    ["./dist/bin/fylo", "exec", "--request", "-"],
    input=json.dumps(request),
    text=True,
    capture_output=True,
    check=False,
)

response = json.loads(proc.stdout)
if proc.returncode != 0 or not response["ok"]:
    raise RuntimeError(response["error"]["message"])

print(response["result"])
```

Go:

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
)

func main() {
	request := map[string]any{
		"requestId":  "inspect-posts-1",
		"op":         "inspectCollection",
		"root":       "/mnt/fylo",
		"collection": "posts",
	}

	body, _ := json.Marshal(request)
	cmd := exec.Command("./dist/bin/fylo", "exec", "--request", "-")
	cmd.Stdin = bytes.NewReader(body)

	output, err := cmd.Output()
	if err != nil {
		panic(err)
	}

	var response map[string]any
	if err := json.Unmarshal(output, &response); err != nil {
		panic(err)
	}
	if response["ok"] != true {
		panic(response["error"])
	}

	fmt.Println(response["result"])
}
```

Rust:

```rust
use serde_json::json;
use std::io::Write;
use std::process::{Command, Stdio};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let request = json!({
        "requestId": "inspect-posts-1",
        "op": "inspectCollection",
        "root": "/mnt/fylo",
        "collection": "posts"
    });

    let mut child = Command::new("./dist/bin/fylo")
        .args(["exec", "--request", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;

    child
        .stdin
        .as_mut()
        .expect("stdin is configured")
        .write_all(request.to_string().as_bytes())?;

    let output = child.wait_with_output()?;
    let response: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    if !output.status.success() || response["ok"] != true {
        return Err(response["error"]["message"].to_string().into());
    }

    println!("{}", response["result"]);
    Ok(())
}
```

Java:

```java
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class FyloExample {
    public static void main(String[] args) throws Exception {
        String request = """
            {
              "requestId": "inspect-posts-1",
              "op": "inspectCollection",
              "root": "/mnt/fylo",
              "collection": "posts"
            }
            """;

        Process process = new ProcessBuilder("./dist/bin/fylo", "exec", "--request", "-")
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .start();

        try (OutputStream stdin = process.getOutputStream()) {
            stdin.write(request.getBytes(StandardCharsets.UTF_8));
        }

        String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exitCode = process.waitFor();
        if (exitCode != 0 || stdout.contains("\"ok\":false")) {
            throw new RuntimeException(stdout);
        }

        System.out.println(stdout);
    }
}
```

Supported machine operations:

- `executeSQL`
- `createCollection`
- `dropCollection`
- `inspectCollection`
- `rebuildCollection`
- `getDoc`
- `getLatest`
- `getHistory`
- `findDocs`
- `joinDocs`
- `putData`
- `batchPutData`
- `patchDoc`
- `patchDocs`
- `delDoc`
- `delDocs`
- `importBulkData`

CLI WORM note:

- pass `--worm` when operating on WORM-enabled collections for commands like `latest`, `history`, and `rebuild`
- `inspect` still reports retained head/version metadata when those files exist on disk

CLI formatting note:

- text output now auto-fits to the terminal width when possible
- long cell values wrap across multiple lines instead of always truncating
- use `--page-size <n>` to repeat headers every `n` rows in text output
- use `--align <left|center|right|auto>` to control cell alignment
- large interactive text output now opens in a pager automatically when FYLO detects a TTY
- use `--no-pager` to force direct stdout output
- set `FYLO_PAGER` to override the pager command, or `FYLO_PAGER=off` / `NO_PAGER=1` to disable paging

## Development

```bash
bun run typecheck
bun run build
bun test
```

Type packaging note:

- FYLO uses JSDoc type modules in `src/` during source development
- `jsconfig.json` is the shared JS/JSDoc source config, matching the pattern used by TACHYON
- `tsconfig.json` extends `jsconfig.json` so `tsc --noEmit` can typecheck the project
- `tsconfig.build.json` exists only for declaration emit into `dist/types`
- published declaration output lives under `dist/types` and is emitted as declaration files only
- type-only source helpers are not copied into the runtime `dist/` tree

## Performance testing

FYLO includes an opt-in scale test for the filesystem engine:

```bash
FYLO_RUN_PERF_TESTS=true bun test tests/integration/filesystem.performance.test.js
```

This is useful when you want to see how index size and query latency behave as collections grow.
