import { mkdir, readFile, readdir, rm, stat, writeFile, open } from 'node:fs/promises'
import path from 'node:path'
import { createHash } from 'node:crypto'
import { Database } from 'bun:sqlite'
import TTID from '@delma/ttid'
import { Dir } from '../core/directory'
import { validateCollectionName } from '../core/collection'
import { Cipher } from '../adapters/cipher'
import type { EventBus, FyloStorageEngineKind, LockManager, StorageEngine } from './types'

type FyloRecord<T extends Record<string, any>> = Record<_ttid, T>

type S3FilesQueryResult<T extends Record<string, any>> =
    | _ttid
    | FyloRecord<T>
    | Record<string, _ttid[]>
    | Record<string, Record<_ttid, Partial<T>>>
    | Record<_ttid, Partial<T>>

type S3FilesEvent<T extends Record<string, any>> = {
    ts: number
    action: 'insert' | 'delete'
    id: _ttid
    doc?: T
}

type StoredDoc<T extends Record<string, any>> = {
    id: _ttid
    createdAt: number
    updatedAt: number
    data: T
}

class FilesystemStorage implements StorageEngine {
    async read(target: string): Promise<string> {
        return await readFile(target, 'utf8')
    }

    async write(target: string, data: string): Promise<void> {
        await mkdir(path.dirname(target), { recursive: true })
        await writeFile(target, data, 'utf8')
    }

    async delete(target: string): Promise<void> {
        await rm(target, { recursive: true, force: true })
    }

    async list(target: string): Promise<string[]> {
        const results: string[] = []

        try {
            const entries = await readdir(target, { withFileTypes: true })
            for (const entry of entries) {
                const child = path.join(target, entry.name)
                if (entry.isDirectory()) {
                    results.push(...(await this.list(child)))
                } else {
                    results.push(child)
                }
            }
        } catch (err) {
            if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err
        }

        return results
    }

    async mkdir(target: string): Promise<void> {
        await mkdir(target, { recursive: true })
    }

    async rmdir(target: string): Promise<void> {
        await rm(target, { recursive: true, force: true })
    }

    async exists(target: string): Promise<boolean> {
        try {
            await stat(target)
            return true
        } catch (err) {
            if ((err as NodeJS.ErrnoException).code === 'ENOENT') return false
            throw err
        }
    }
}

class FilesystemLockManager implements LockManager {
    constructor(
        private readonly root: string,
        private readonly storage: StorageEngine
    ) {}

    private lockDir(collection: string, docId: _ttid) {
        return path.join(this.root, collection, '.fylo', 'locks', `${docId}.lock`)
    }

    async acquire(
        collection: string,
        docId: _ttid,
        owner: string,
        ttlMs: number = 30_000
    ): Promise<boolean> {
        const dir = this.lockDir(collection, docId)
        const metaPath = path.join(dir, 'meta.json')
        await mkdir(path.dirname(dir), { recursive: true })

        try {
            await mkdir(dir, { recursive: false })
            await this.storage.write(metaPath, JSON.stringify({ owner, ts: Date.now() }))
            return true
        } catch (err) {
            if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err
        }

        try {
            const meta = JSON.parse(await this.storage.read(metaPath)) as { ts?: number }
            if (meta.ts && Date.now() - meta.ts > ttlMs) {
                await this.storage.rmdir(dir)
                await mkdir(dir, { recursive: false })
                await this.storage.write(metaPath, JSON.stringify({ owner, ts: Date.now() }))
                return true
            }
        } catch {
            await this.storage.rmdir(dir)
            await mkdir(dir, { recursive: false })
            await this.storage.write(metaPath, JSON.stringify({ owner, ts: Date.now() }))
            return true
        }

        return false
    }

    async release(collection: string, docId: _ttid, owner: string): Promise<void> {
        const dir = this.lockDir(collection, docId)
        const metaPath = path.join(dir, 'meta.json')

        try {
            const meta = JSON.parse(await this.storage.read(metaPath)) as { owner?: string }
            if (meta.owner === owner) await this.storage.rmdir(dir)
        } catch (err) {
            if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err
        }
    }
}

class FilesystemEventBus<T extends Record<string, any>> implements EventBus<S3FilesEvent<T>> {
    constructor(
        private readonly root: string,
        private readonly storage: StorageEngine
    ) {}

    private journalPath(collection: string) {
        return path.join(this.root, collection, '.fylo', 'events', `${collection}.ndjson`)
    }

    async publish(collection: string, event: S3FilesEvent<T>): Promise<void> {
        const target = this.journalPath(collection)
        await mkdir(path.dirname(target), { recursive: true })
        const line = `${JSON.stringify(event)}\n`
        const handle = await open(target, 'a')
        try {
            await handle.write(line)
        } finally {
            await handle.close()
        }
    }

    async *listen(collection: string): AsyncGenerator<S3FilesEvent<T>, void, unknown> {
        const target = this.journalPath(collection)
        let position = 0

        while (true) {
            try {
                const fileStat = await stat(target)
                if (fileStat.size > position) {
                    const handle = await open(target, 'r')
                    try {
                        const size = fileStat.size - position
                        const buffer = Buffer.alloc(size)
                        await handle.read(buffer, 0, size, position)
                        position = fileStat.size

                        for (const line of buffer.toString('utf8').split('\n')) {
                            if (line.trim().length === 0) continue
                            yield JSON.parse(line) as S3FilesEvent<T>
                        }
                    } finally {
                        await handle.close()
                    }
                }
            } catch (err) {
                if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err
            }

            await Bun.sleep(100)
        }
    }
}

export class S3FilesEngine {
    readonly kind: FyloStorageEngineKind = 's3-files'

    private readonly databases = new Map<string, Database>()

    private readonly storage: StorageEngine
    private readonly locks: LockManager
    private readonly events: EventBus<Record<string, any>>

    constructor(readonly root: string = process.env.FYLO_S3FILES_ROOT ?? '/mnt/fylo') {
        this.storage = new FilesystemStorage()
        this.locks = new FilesystemLockManager(this.root, this.storage)
        this.events = new FilesystemEventBus<Record<string, any>>(this.root, this.storage)
    }

    private collectionRoot(collection: string) {
        validateCollectionName(collection)
        return path.join(this.root, collection)
    }

    private docsRoot(collection: string) {
        return path.join(this.collectionRoot(collection), '.fylo', 'docs')
    }

    private metaRoot(collection: string) {
        return path.join(this.collectionRoot(collection), '.fylo')
    }

    private indexDbPath(collection: string) {
        return path.join(this.metaRoot(collection), 'index.db')
    }

    private docPath(collection: string, docId: _ttid) {
        return path.join(this.docsRoot(collection), docId.slice(0, 2), `${docId}.json`)
    }

    private hash(value: string) {
        return createHash('sha256').update(value).digest('hex')
    }

    private database(collection: string) {
        const existing = this.databases.get(collection)
        if (existing) return existing

        const db = new Database(this.indexDbPath(collection))
        db.exec(`
            CREATE TABLE IF NOT EXISTS doc_index_entries (
                doc_id TEXT NOT NULL,
                field_path TEXT NOT NULL,
                value_hash TEXT NOT NULL,
                raw_value TEXT NOT NULL,
                value_type TEXT NOT NULL,
                numeric_value REAL,
                PRIMARY KEY (doc_id, field_path, value_hash)
            );

            CREATE INDEX IF NOT EXISTS idx_doc_index_entries_field_hash
            ON doc_index_entries (field_path, value_hash);

            CREATE INDEX IF NOT EXISTS idx_doc_index_entries_field_numeric
            ON doc_index_entries (field_path, numeric_value);
        `)
        this.databases.set(collection, db)
        return db
    }

    private closeDatabase(collection: string) {
        const db = this.databases.get(collection)
        if (db) {
            db.close()
            this.databases.delete(collection)
        }
    }

    private normalizeIndexValue(rawValue: string) {
        const parsed = Dir.parseValue(rawValue.replaceAll('%2F', '/'))
        const numeric = typeof parsed === 'number' ? parsed : Number(parsed)
        return {
            rawValue,
            valueHash: this.hash(rawValue),
            valueType: typeof parsed,
            numericValue: Number.isNaN(numeric) ? null : numeric
        }
    }

    private async ensureCollection(collection: string) {
        await this.storage.mkdir(this.collectionRoot(collection))
        await this.storage.mkdir(this.metaRoot(collection))
        await this.storage.mkdir(this.docsRoot(collection))
        this.database(collection)
    }

    async createCollection(collection: string) {
        await this.ensureCollection(collection)
    }

    async dropCollection(collection: string) {
        this.closeDatabase(collection)
        await this.storage.rmdir(this.collectionRoot(collection))
    }

    async hasCollection(collection: string) {
        return await this.storage.exists(this.collectionRoot(collection))
    }

    private async encodeEncrypted<T extends Record<string, any>>(
        collection: string,
        value: T,
        parentField?: string
    ): Promise<T> {
        if (Array.isArray(value)) {
            const encodedItems = await Promise.all(
                value.map(async (item) => {
                    if (item && typeof item === 'object')
                        return await this.encodeEncrypted(collection, item as Record<string, any>)
                    if (
                        parentField &&
                        Cipher.isConfigured() &&
                        Cipher.isEncryptedField(collection, parentField)
                    ) {
                        return await Cipher.encrypt(String(item).replaceAll('/', '%2F'), true)
                    }
                    return item
                })
            )
            return encodedItems as unknown as T
        }

        if (value && typeof value === 'object') {
            const copy: Record<string, any> = {}
            for (const field in value) {
                const nextField = parentField ? `${parentField}/${field}` : field
                const fieldValue = value[field]
                if (fieldValue && typeof fieldValue === 'object')
                    copy[field] = await this.encodeEncrypted(collection, fieldValue, nextField)
                else if (Cipher.isConfigured() && Cipher.isEncryptedField(collection, nextField)) {
                    copy[field] = await Cipher.encrypt(
                        String(fieldValue).replaceAll('/', '%2F'),
                        true
                    )
                } else copy[field] = fieldValue
            }
            return copy as T
        }

        return value
    }

    private async decodeEncrypted<T extends Record<string, any>>(
        collection: string,
        value: T,
        parentField?: string
    ): Promise<T> {
        if (Array.isArray(value)) {
            const decodedItems = await Promise.all(
                value.map(async (item) => {
                    if (item && typeof item === 'object')
                        return await this.decodeEncrypted(collection, item as Record<string, any>)
                    if (
                        parentField &&
                        Cipher.isConfigured() &&
                        Cipher.isEncryptedField(collection, parentField) &&
                        typeof item === 'string'
                    ) {
                        return Dir.parseValue((await Cipher.decrypt(item)).replaceAll('%2F', '/'))
                    }
                    return item
                })
            )
            return decodedItems as unknown as T
        }

        if (value && typeof value === 'object') {
            const copy: Record<string, any> = {}
            for (const field in value) {
                const nextField = parentField ? `${parentField}/${field}` : field
                const fieldValue = value[field]
                if (fieldValue && typeof fieldValue === 'object')
                    copy[field] = await this.decodeEncrypted(collection, fieldValue, nextField)
                else if (
                    Cipher.isConfigured() &&
                    Cipher.isEncryptedField(collection, nextField) &&
                    typeof fieldValue === 'string'
                ) {
                    copy[field] = Dir.parseValue(
                        (await Cipher.decrypt(fieldValue)).replaceAll('%2F', '/')
                    )
                } else copy[field] = fieldValue
            }
            return copy as T
        }

        return value
    }

    private async readStoredDoc<T extends Record<string, any>>(
        collection: string,
        docId: _ttid
    ): Promise<StoredDoc<T> | null> {
        const target = this.docPath(collection, docId)

        try {
            const raw = JSON.parse(await this.storage.read(target)) as StoredDoc<T>
            raw.data = await this.decodeEncrypted(collection, raw.data)
            return raw
        } catch (err) {
            if ((err as NodeJS.ErrnoException).code === 'ENOENT') return null
            throw err
        }
    }

    private async writeStoredDoc<T extends Record<string, any>>(
        collection: string,
        docId: _ttid,
        data: T
    ) {
        await this.ensureCollection(collection)
        const encoded = await this.encodeEncrypted(collection, data)
        const { createdAt, updatedAt } = TTID.decodeTime(docId)
        const target = this.docPath(collection, docId)
        const record: StoredDoc<T> = {
            id: docId,
            createdAt,
            updatedAt: updatedAt ?? createdAt,
            data: encoded
        }
        await this.storage.write(target, JSON.stringify(record))
    }

    private async removeStoredDoc(collection: string, docId: _ttid) {
        await this.storage.delete(this.docPath(collection, docId))
    }

    private async listDocIds(collection: string) {
        const files = await this.storage.list(this.docsRoot(collection))
        return files
            .filter((file) => file.endsWith('.json'))
            .map((file) => path.basename(file, '.json'))
            .filter((key) => TTID.isTTID(key)) as _ttid[]
    }

    private getValueByPath(target: Record<string, any>, fieldPath: string) {
        return fieldPath
            .replaceAll('/', '.')
            .split('.')
            .reduce<any>(
                (acc, key) => (acc === undefined || acc === null ? undefined : acc[key]),
                target
            )
    }

    private normalizeFieldPath(fieldPath: string) {
        return fieldPath.replaceAll('.', '/')
    }

    private matchesTimestamp(docId: _ttid, query?: _storeQuery<Record<string, any>>) {
        if (!query?.$created && !query?.$updated) return true
        const { createdAt, updatedAt } = TTID.decodeTime(docId)
        const timestamps = { createdAt, updatedAt: updatedAt ?? createdAt }

        const match = (value: number, range?: _timestamp) => {
            if (!range) return true
            if (range.$gt !== undefined && !(value > range.$gt)) return false
            if (range.$gte !== undefined && !(value >= range.$gte)) return false
            if (range.$lt !== undefined && !(value < range.$lt)) return false
            if (range.$lte !== undefined && !(value <= range.$lte)) return false
            return true
        }

        return (
            match(timestamps.createdAt, query.$created) &&
            match(timestamps.updatedAt, query.$updated)
        )
    }

    private likeToRegex(pattern: string) {
        const escaped = pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replaceAll('%', '.*')
        return new RegExp(`^${escaped}$`)
    }

    private matchesOperand(value: unknown, operand: _operand) {
        if (operand.$eq !== undefined && value != operand.$eq) return false
        if (operand.$ne !== undefined && value == operand.$ne) return false
        if (operand.$gt !== undefined && !(Number(value) > operand.$gt)) return false
        if (operand.$gte !== undefined && !(Number(value) >= operand.$gte)) return false
        if (operand.$lt !== undefined && !(Number(value) < operand.$lt)) return false
        if (operand.$lte !== undefined && !(Number(value) <= operand.$lte)) return false
        if (
            operand.$like !== undefined &&
            (typeof value !== 'string' || !this.likeToRegex(operand.$like).test(value))
        )
            return false
        if (operand.$contains !== undefined) {
            if (!Array.isArray(value) || !value.some((item) => item == operand.$contains))
                return false
        }
        return true
    }

    private async normalizeQueryValue(collection: string, fieldPath: string, value: unknown) {
        let rawValue = String(value).replaceAll('/', '%2F')
        if (Cipher.isConfigured() && Cipher.isEncryptedField(collection, fieldPath))
            rawValue = await Cipher.encrypt(rawValue, true)
        return this.normalizeIndexValue(rawValue)
    }

    private intersectDocIds(current: Set<_ttid> | null, next: Iterable<_ttid>) {
        const nextSet = next instanceof Set ? next : new Set(next)
        if (current === null) return new Set(nextSet)

        const intersection = new Set<_ttid>()
        for (const docId of current) {
            if (nextSet.has(docId)) intersection.add(docId)
        }
        return intersection
    }

    private async queryDocIdsBySql(
        collection: string,
        sql: string,
        ...params: unknown[]
    ): Promise<Set<_ttid>> {
        const db = this.database(collection)
        const rows = db
            .query(sql)
            .all(...params)
            .map((row) => (row as { doc_id: _ttid }).doc_id)

        return new Set(rows)
    }

    private async candidateDocIdsForOperand(
        collection: string,
        fieldPath: string,
        operand: _operand
    ): Promise<Set<_ttid> | null> {
        if (Cipher.isConfigured() && Cipher.isEncryptedField(collection, fieldPath)) return null

        let candidateIds: Set<_ttid> | null = null

        if (operand.$eq !== undefined) {
            const normalized = await this.normalizeQueryValue(collection, fieldPath, operand.$eq)
            candidateIds = this.intersectDocIds(
                candidateIds,
                await this.queryDocIdsBySql(
                    collection,
                    `SELECT DISTINCT doc_id
                     FROM doc_index_entries
                     WHERE field_path = ? AND value_hash = ?`,
                    fieldPath,
                    normalized.valueHash
                )
            )
        }

        if (
            operand.$gt !== undefined ||
            operand.$gte !== undefined ||
            operand.$lt !== undefined ||
            operand.$lte !== undefined
        ) {
            const clauses = ['field_path = ?']
            const params: unknown[] = [fieldPath]
            if (operand.$gt !== undefined) {
                clauses.push('numeric_value > ?')
                params.push(operand.$gt)
            }
            if (operand.$gte !== undefined) {
                clauses.push('numeric_value >= ?')
                params.push(operand.$gte)
            }
            if (operand.$lt !== undefined) {
                clauses.push('numeric_value < ?')
                params.push(operand.$lt)
            }
            if (operand.$lte !== undefined) {
                clauses.push('numeric_value <= ?')
                params.push(operand.$lte)
            }

            candidateIds = this.intersectDocIds(
                candidateIds,
                await this.queryDocIdsBySql(
                    collection,
                    `SELECT DISTINCT doc_id
                     FROM doc_index_entries
                     WHERE ${clauses.join(' AND ')}`,
                    ...params
                )
            )
        }

        if (operand.$like !== undefined) {
            candidateIds = this.intersectDocIds(
                candidateIds,
                await this.queryDocIdsBySql(
                    collection,
                    `SELECT DISTINCT doc_id
                     FROM doc_index_entries
                     WHERE field_path = ? AND value_type = 'string' AND raw_value LIKE ?`,
                    fieldPath,
                    operand.$like.replaceAll('/', '%2F')
                )
            )
        }

        if (operand.$contains !== undefined) {
            const normalized = await this.normalizeQueryValue(
                collection,
                fieldPath,
                operand.$contains
            )
            candidateIds = this.intersectDocIds(
                candidateIds,
                await this.queryDocIdsBySql(
                    collection,
                    `SELECT DISTINCT doc_id
                     FROM doc_index_entries
                     WHERE (field_path = ? OR field_path LIKE ?)
                       AND value_hash = ?`,
                    fieldPath,
                    `${fieldPath}/%`,
                    normalized.valueHash
                )
            )
        }

        return candidateIds
    }

    private async candidateDocIdsForOperation<T extends Record<string, any>>(
        collection: string,
        operation: _op<T>
    ): Promise<Set<_ttid> | null> {
        let candidateIds: Set<_ttid> | null = null

        for (const [field, operand] of Object.entries(operation) as Array<[keyof T, _operand]>) {
            if (!operand) continue

            const fieldPath = this.normalizeFieldPath(String(field))
            const fieldCandidates = await this.candidateDocIdsForOperand(
                collection,
                fieldPath,
                operand
            )

            if (fieldCandidates === null) continue
            candidateIds = this.intersectDocIds(candidateIds, fieldCandidates)
        }

        return candidateIds
    }

    private async candidateDocIdsForQuery<T extends Record<string, any>>(
        collection: string,
        query?: _storeQuery<T>
    ): Promise<Set<_ttid> | null> {
        if (!query?.$ops || query.$ops.length === 0) return null

        const union = new Set<_ttid>()
        let usedIndex = false

        for (const operation of query.$ops) {
            const candidateIds = await this.candidateDocIdsForOperation(collection, operation)
            if (candidateIds === null) return null
            usedIndex = true
            for (const docId of candidateIds) union.add(docId)
        }

        return usedIndex ? union : null
    }

    private matchesQuery<T extends Record<string, any>>(
        docId: _ttid,
        doc: T,
        query?: _storeQuery<T>
    ) {
        if (!this.matchesTimestamp(docId, query as _storeQuery<Record<string, any>> | undefined))
            return false
        if (!query?.$ops || query.$ops.length === 0) return true

        return query.$ops.some((operation) => {
            for (const field in operation) {
                const value = this.getValueByPath(doc, field)
                if (!this.matchesOperand(value, operation[field as keyof T]!)) return false
            }
            return true
        })
    }

    private selectValues<T extends Record<string, any>>(selection: Array<keyof T>, data: T) {
        const copy = { ...data }
        for (const field in copy) {
            if (!selection.includes(field as keyof T)) delete copy[field]
        }
        return copy
    }

    private renameFields<T extends Record<string, any>>(
        rename: Record<keyof Partial<T>, string>,
        data: T
    ) {
        const copy = { ...data }
        for (const field in copy) {
            if (rename[field]) {
                copy[rename[field]] = copy[field]
                delete copy[field]
            }
        }
        return copy
    }

    private processDoc<T extends Record<string, any>>(
        doc: FyloRecord<T>,
        query?: _storeQuery<T>
    ): S3FilesQueryResult<T> | undefined {
        if (Object.keys(doc).length === 0) return

        const next = { ...doc }

        for (let [_id, data] of Object.entries(next)) {
            if (query?.$select?.length)
                data = this.selectValues(query.$select as Array<keyof T>, data)
            if (query?.$rename) data = this.renameFields(query.$rename, data)
            next[_id as _ttid] = data as T
        }

        if (query?.$groupby) {
            const docGroup: Record<string, Record<string, Partial<T>>> = {}
            for (const [id, data] of Object.entries(next)) {
                const groupValue = data[query.$groupby] as string
                if (groupValue) {
                    const groupData = { ...data }
                    delete groupData[query.$groupby]
                    docGroup[groupValue] = { [id]: groupData as Partial<T> }
                }
            }

            if (query.$onlyIds) {
                const groupedIds: Record<string, _ttid[]> = {}
                for (const group in docGroup)
                    groupedIds[group] = Object.keys(docGroup[group]) as _ttid[]
                return groupedIds
            }

            return docGroup
        }

        if (query?.$onlyIds) return Object.keys(next).shift() as _ttid

        return next
    }

    private async docResults<T extends Record<string, any>>(
        collection: string,
        query?: _storeQuery<T>
    ) {
        const candidateIds = await this.candidateDocIdsForQuery(collection, query)
        const ids = candidateIds ? Array.from(candidateIds) : await this.listDocIds(collection)
        const limit = query?.$limit
        const results: Array<FyloRecord<T>> = []

        for (const id of ids) {
            const stored = await this.readStoredDoc<T>(collection, id)
            if (!stored) continue
            if (!this.matchesQuery(id, stored.data, query)) continue
            results.push({ [id]: stored.data } as FyloRecord<T>)
            if (limit && results.length >= limit) break
        }

        return results
    }

    private async rebuildIndexes<T extends Record<string, any>>(
        collection: string,
        docId: _ttid,
        doc: T
    ) {
        const keys = await Dir.extractKeys(collection, docId, doc)
        const db = this.database(collection)
        const insert = db.query(`
            INSERT OR REPLACE INTO doc_index_entries
            (doc_id, field_path, value_hash, raw_value, value_type, numeric_value)
            VALUES (?, ?, ?, ?, ?, ?)
        `)

        const transaction = db.transaction((logicalKeys: string[]) => {
            for (const logicalKey of logicalKeys) {
                const segments = logicalKey.split('/')
                const fieldPath = segments.slice(0, -2).join('/')
                const rawValue = segments.at(-2) ?? ''
                const normalized = this.normalizeIndexValue(rawValue)
                insert.run(
                    docId,
                    fieldPath,
                    normalized.valueHash,
                    normalized.rawValue,
                    normalized.valueType,
                    normalized.numericValue
                )
            }
        })

        transaction(keys.indexes)
    }

    private async removeIndexes<T extends Record<string, any>>(
        collection: string,
        docId: _ttid,
        doc: T
    ) {
        const keys = await Dir.extractKeys(collection, docId, doc)
        const db = this.database(collection)
        const remove = db.query(`
            DELETE FROM doc_index_entries
            WHERE doc_id = ? AND field_path = ? AND value_hash = ?
        `)

        const transaction = db.transaction((logicalKeys: string[]) => {
            for (const logicalKey of logicalKeys) {
                const segments = logicalKey.split('/')
                const fieldPath = segments.slice(0, -2).join('/')
                const rawValue = segments.at(-2) ?? ''
                remove.run(docId, fieldPath, this.hash(rawValue))
            }
        })

        transaction(keys.indexes)
    }

    async putDocument<T extends Record<string, any>>(collection: string, docId: _ttid, doc: T) {
        const owner = Bun.randomUUIDv7()
        if (!(await this.locks.acquire(collection, docId, owner)))
            throw new Error(`Unable to acquire filesystem lock for ${docId}`)

        try {
            await this.writeStoredDoc(collection, docId, doc)
            await this.rebuildIndexes(collection, docId, doc)
            await this.events.publish(collection, {
                ts: Date.now(),
                action: 'insert',
                id: docId,
                doc
            })
        } finally {
            await this.locks.release(collection, docId, owner)
        }
    }

    async patchDocument<T extends Record<string, any>>(
        collection: string,
        oldId: _ttid,
        newId: _ttid,
        patch: Partial<T>,
        oldDoc?: T
    ) {
        const owner = Bun.randomUUIDv7()
        if (!(await this.locks.acquire(collection, oldId, owner)))
            throw new Error(`Unable to acquire filesystem lock for ${oldId}`)

        try {
            const existing = oldDoc ?? (await this.readStoredDoc<T>(collection, oldId))?.data
            if (!existing) return oldId

            const nextDoc = { ...existing, ...patch } as T
            await this.removeIndexes(collection, oldId, existing)
            await this.removeStoredDoc(collection, oldId)
            await this.events.publish(collection, {
                ts: Date.now(),
                action: 'delete',
                id: oldId,
                doc: existing
            })
            await this.writeStoredDoc(collection, newId, nextDoc)
            await this.rebuildIndexes(collection, newId, nextDoc)
            await this.events.publish(collection, {
                ts: Date.now(),
                action: 'insert',
                id: newId,
                doc: nextDoc
            })
            return newId
        } finally {
            await this.locks.release(collection, oldId, owner)
        }
    }

    async deleteDocument<T extends Record<string, any>>(collection: string, docId: _ttid) {
        const owner = Bun.randomUUIDv7()
        if (!(await this.locks.acquire(collection, docId, owner)))
            throw new Error(`Unable to acquire filesystem lock for ${docId}`)

        try {
            const existing = await this.readStoredDoc<T>(collection, docId)
            if (!existing) return
            await this.removeIndexes(collection, docId, existing.data)
            await this.removeStoredDoc(collection, docId)
            await this.events.publish(collection, {
                ts: Date.now(),
                action: 'delete',
                id: docId,
                doc: existing.data
            })
        } finally {
            await this.locks.release(collection, docId, owner)
        }
    }

    getDoc<T extends Record<string, any>>(
        collection: string,
        docId: _ttid,
        onlyId: boolean = false
    ) {
        const engine = this

        return {
            async *[Symbol.asyncIterator]() {
                const doc = await this.once()
                if (Object.keys(doc).length > 0) yield onlyId ? Object.keys(doc).shift()! : doc

                for await (const event of engine.events.listen(collection)) {
                    if (event.action !== 'insert' || event.id !== docId || !event.doc) continue
                    yield onlyId ? event.id : ({ [event.id]: event.doc } as FyloRecord<T>)
                }
            },
            async once() {
                const stored = await engine.readStoredDoc<T>(collection, docId)
                return stored ? ({ [docId]: stored.data } as FyloRecord<T>) : {}
            },
            async *onDelete() {
                for await (const event of engine.events.listen(collection)) {
                    if (event.action === 'delete' && event.id === docId) yield event.id
                }
            }
        }
    }

    findDocs<T extends Record<string, any>>(collection: string, query?: _storeQuery<T>) {
        const engine = this

        const collectDocs = async function* () {
            const docs = await engine.docResults(collection, query)
            for (const doc of docs) {
                const result = engine.processDoc(doc, query)
                if (result !== undefined) yield result
            }
        }

        return {
            async *[Symbol.asyncIterator]() {
                for await (const result of collectDocs()) yield result

                for await (const event of engine.events.listen(collection)) {
                    if (event.action !== 'insert' || !event.doc) continue
                    if (!engine.matchesQuery(event.id, event.doc as T, query)) continue
                    const processed = engine.processDoc(
                        { [event.id]: event.doc as T } as FyloRecord<T>,
                        query
                    )
                    if (processed !== undefined) yield processed
                }
            },
            async *collect() {
                for await (const result of collectDocs()) yield result
            },
            async *onDelete() {
                for await (const event of engine.events.listen(collection)) {
                    if (event.action !== 'delete' || !event.doc) continue
                    if (!engine.matchesQuery(event.id, event.doc as T, query)) continue
                    yield event.id
                }
            }
        }
    }

    async *exportBulkData<T extends Record<string, any>>(collection: string) {
        const ids = await this.listDocIds(collection)
        for (const id of ids) {
            const stored = await this.readStoredDoc<T>(collection, id)
            if (stored) yield stored.data
        }
    }

    async joinDocs<T extends Record<string, any>, U extends Record<string, any>>(
        join: _join<T, U>
    ) {
        const leftDocs = await this.docResults<T>(join.$leftCollection)
        const rightDocs = await this.docResults<U>(join.$rightCollection)
        const docs: Record<`${_ttid}, ${_ttid}`, T | U | (T & U) | (Partial<T> & Partial<U>)> = {}

        const compareMap = {
            $eq: (leftVal: any, rightVal: any) => leftVal === rightVal,
            $ne: (leftVal: any, rightVal: any) => leftVal !== rightVal,
            $gt: (leftVal: any, rightVal: any) => Number(leftVal) > Number(rightVal),
            $lt: (leftVal: any, rightVal: any) => Number(leftVal) < Number(rightVal),
            $gte: (leftVal: any, rightVal: any) => Number(leftVal) >= Number(rightVal),
            $lte: (leftVal: any, rightVal: any) => Number(leftVal) <= Number(rightVal)
        } as const

        for (const leftEntry of leftDocs) {
            const [leftId, leftData] = Object.entries(leftEntry)[0] as [_ttid, T]
            for (const rightEntry of rightDocs) {
                const [rightId, rightData] = Object.entries(rightEntry)[0] as [_ttid, U]

                let matched = false

                for (const field in join.$on) {
                    const operand = join.$on[field as keyof T]!
                    for (const opKey of Object.keys(compareMap) as Array<keyof typeof compareMap>) {
                        const rightField = operand[opKey]
                        if (!rightField) continue
                        const leftValue = this.getValueByPath(
                            leftData as Record<string, any>,
                            String(field)
                        )
                        const rightValue = this.getValueByPath(
                            rightData as Record<string, any>,
                            String(rightField)
                        )
                        if (compareMap[opKey](leftValue, rightValue)) matched = true
                    }
                }

                if (!matched) continue

                switch (join.$mode) {
                    case 'inner':
                        docs[`${leftId}, ${rightId}`] = { ...leftData, ...rightData } as T & U
                        break
                    case 'left':
                        docs[`${leftId}, ${rightId}`] = leftData
                        break
                    case 'right':
                        docs[`${leftId}, ${rightId}`] = rightData
                        break
                    case 'outer':
                        docs[`${leftId}, ${rightId}`] = { ...leftData, ...rightData } as T & U
                        break
                }

                if (join.$limit && Object.keys(docs).length >= join.$limit) break
            }

            if (join.$limit && Object.keys(docs).length >= join.$limit) break
        }

        if (join.$groupby) {
            const groupedDocs: Record<string, Record<string, Partial<T | U>>> = {}
            for (const ids in docs) {
                const data = docs[ids as `${_ttid}, ${_ttid}`] as Record<string, any>
                const key = String(data[join.$groupby as string])
                if (!groupedDocs[key]) groupedDocs[key] = {}
                groupedDocs[key][ids] = data as Partial<T | U>
            }
            if (join.$onlyIds) {
                const groupedIds: Record<string, _ttid[]> = {}
                for (const key in groupedDocs)
                    groupedIds[key] = Object.keys(groupedDocs[key]).flat() as _ttid[]
                return groupedIds
            }
            return groupedDocs
        }

        if (join.$onlyIds) return Array.from(new Set(Object.keys(docs).flat())) as _ttid[]

        return docs
    }
}
