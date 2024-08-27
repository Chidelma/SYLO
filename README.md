# BYOS - Bring Your Own Storage

BYOS is a customizable storage solution built with TypeScript (Bun), providing an interface to interact with various AWS S3-Compactable storage services. It allows you to easily integrate your own storage solution with your application, providing a simple and intuitive interface for managing and accessing your data.

## Features

- Support for various AWS S3-Compactable storage services
- Customizable storage solutions
- Easy integration with your application
- Simple and intuitive CLI interface for managing and accessing your data

## Installation

```bash
bun add @vyckr/byos
```

## Configuration

The .env file should be in the root directory of your project. The following environment variables:
```
DB_DIR=/path/to/disk/database (required)
SCHEMA=LOOSE|STRICT (optional)
LOGGING=true|false (optional)
SCHEMA_PATH=/path/to/schema/directory (required if SCHEMA is set to STRICT)
MEM_DR=/path/to/memory/database (optional)
S3_REGION=region (optional)
S3_INDEX_BUCKET=bucket (required)
S3_DATA_BUCKET=bucket (required)
S3_ENDPOINT=https//example.com (optional)
```

## Usage/Example

Make sure you have set the 'SCHEMA_PATH' if 'SCHEMA' is set to 'STRICT'. The schema path should be a directory containing the declaration files. for example:

```
/path/to/schema/directory
    /users.d.ts
```

```typescript
import Silo from "@vyckr/byos";

await Silo.createSchema("users")

const _id = await Silo.putData<_user>("users", { name: "John Doe", age: 30 })

const user = await Silo.getDoc<_user>("users", _id).once()

console.log(user)

await Silo.importBulkData<_user>("users", new URL("https://example.com/users.json"), 100)

for await (const user of Silo.findDocs<_user>("users", { $limit: 10 }).collect()) {
    console.log(user)
}

await Silo.patchDoc<_user>("users", new Map([[_id, { name: "Jane Doe" }]]))

const count = await Silo.patchDocs<_user>("users", { $set: { age: 31 } })

console.log("Updated", count)

await Silo.delDoc<_user>("users", _id)

const count = await Silo.delDocs<_user>("users", { $ops: [ { name: { $like: "%Doe%" } } ] })

console.log("Deleted", count)

await Silo.dropSchema("users")
```

The equivalent of the above code using SQL syntax would be:

```typescript
import Silo from "@vyckr/byos";

await Silo.executeSQL<_user>(`CREATE TABLE users`)

const _id = await Silo.executeSQL<_user>(`INSERT INTO users (name, age) VALUES ('John Doe'|30)`)

let docs = await Silo.executeSQL<_user>(`SELECT * FROM users WHERE id = ${_id}`)

console.log(docs)

docs = await Silo.executeSQL<_user>(`SELECT * FROM users LIMIT 10`)

console.log(docs)

let count = await Silo.executeSQL<_user>(`UPDATE users SET age = 31 WHERE id = ${_id}`)

console.log("Updated", count)

const count = await Silo.executeSQL<_user>(`DELETE FROM users WHERE name LIKE '%Doe%'`)

console.log("Deleted", count)

await Silo.executeSQL<_user>(`DROP TABLE users`)
```

For streaming (listening) data, you can use the following methods:

```typescript
import Silo from "@vyckr/byos";

for await (const user of Silo.findDocs<_user>("users", { $limit: 10 })) {
    console.log(user)
}

for await (const _id of Silo.findDocs<_user>("users", { $limit: 10 }).onDelete()) {
    console.log(_id)
}

for await (const user of Silo.getDoc<_user>("users", _id)) {
    console.log(user)
}

for await (const _id of Silo.getDoc<_user>("users", _id).onDelete()) {
    console.log(_id)
}
```

# License

BYOS is licensed under the MIT License.