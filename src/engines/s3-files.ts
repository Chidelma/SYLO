import { rename, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { createHash } from 'node:crypto'
import TTID from '@delma/ttid'
import { Dir } from '../core/directory'
import { validateCollectionName } from '../core/collection'
import { assertPathInside, validateDocId } from '../core/doc-id'
import { Cipher } from '../adapters/cipher'
import {
    FyloSyncError,
    resolveSyncMode,
    type FyloDeleteSyncEvent,
    type FyloSyncHooks,
    type FyloSyncMode,
    type FyloWriteSyncEvent
} from '../sync'
import type { EventBus, FyloStorageEngineKind, LockManager, StorageEngine } from './types'
import {
    type CollectionIndexCache,
    type FyloRecord,
    type StoredCollectionIndex,
    type StoredIndexEntry
} from './s3-files/types'
import { FilesystemEventBus, FilesystemLockManager, FilesystemStorage } from './s3-files/filesystem'
import { S3FilesDocuments } from './s3-files/documents'
import { S3FilesQueryEngine } from './s3-files/query'

export class S3FilesEngine {
    readonly kind: FyloStorageEngineKind = 's3-files'

    private readonly indexes = new Map<string, CollectionIndexCache>()
    private readonly writeLanes = new Map<string, Promise<void>>()

    private readonly storage: StorageEngine
    private readonly locks: LockManager
    private readonly events: EventBus<Record<string, any>>
    private readonly documents: S3FilesDocuments
    private readonly queryEngine: S3FilesQueryEngine
    private readonly sync?: FyloSyncHooks
    private readonly syncMode: FyloSyncMode

    constructor(
        readonly root: string = process.env.FYLO_ROOT ??
            process.env.FYLO_S3FILES_ROOT ??
            path.join(process.cwd(), '.fylo-data'),
        options: {
            sync?: FyloSyncHooks
            syncMode?: FyloSyncMode
        } = {}
    ) {
        this.sync = options.sync
        this.syncMode = resolveSyncMode(options.syncMode)
        this.storage = new FilesystemStorage()
        this.locks = new FilesystemLockManager(this.root, this.storage)
        this.events = new FilesystemEventBus<Record<string, any>>(this.root, this.storage)
        this.documents = new S3FilesDocuments(
            this.storage,
            this.docsRoot.bind(this),
            this.docPath.bind(this),
            this.ensureCollection.bind(this),
            this.encodeEncrypted.bind(this),
            this.decodeEncrypted.bind(this)
        )
        this.queryEngine = new S3FilesQueryEngine({
            loadIndexCache: this.loadIndexCache.bind(this),
            normalizeIndexValue: this.normalizeIndexValue.bind(this)
        })
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

    private indexesRoot(collection: string) {
        return path.join(this.metaRoot(collection), 'indexes')
    }

    private indexFilePath(collection: string) {
        return path.join(this.indexesRoot(collection), `${collection}.idx.json`)
    }

    private docPath(collection: string, docId: _ttid) {
        validateDocId(docId)
        const docsRoot = this.docsRoot(collection)
        const target = path.join(docsRoot, docId.slice(0, 2), `${docId}.json`)
        assertPathInside(docsRoot, target)
        return target
    }

    private async runSyncTask(
        collection: string,
        docId: _ttid,
        operation: string,
        targetPath: string,
        task: () => Promise<void>
    ) {
        if (!this.sync?.onWrite && !this.sync?.onDelete) return

        if (this.syncMode === 'fire-and-forget') {
            void task().catch((cause) => {
                console.error(
                    new FyloSyncError({
                        collection,
                        docId,
                        operation,
                        path: targetPath,
                        cause
                    })
                )
            })
            return
        }

        try {
            await task()
        } catch (cause) {
            throw new FyloSyncError({
                collection,
                docId,
                operation,
                path: targetPath,
                cause
            })
        }
    }

    private async syncWrite<T extends Record<string, any>>(event: FyloWriteSyncEvent<T>) {
        if (!this.sync?.onWrite) return
        await this.sync.onWrite(event)
    }

    private async syncDelete(event: FyloDeleteSyncEvent) {
        if (!this.sync?.onDelete) return
        await this.sync.onDelete(event)
    }

    private hash(value: string) {
        return createHash('sha256').update(value).digest('hex')
    }

    private createEmptyIndexCache(): CollectionIndexCache {
        return {
            docs: new Map(),
            fieldHash: new Map(),
            fieldNumeric: new Map(),
            fieldString: new Map()
        }
    }

    private addEntryToCache(cache: CollectionIndexCache, docId: _ttid, entry: StoredIndexEntry) {
        let valueHashBucket = cache.fieldHash.get(entry.fieldPath)
        if (!valueHashBucket) {
            valueHashBucket = new Map()
            cache.fieldHash.set(entry.fieldPath, valueHashBucket)
        }

        let docsForValue = valueHashBucket.get(entry.valueHash)
        if (!docsForValue) {
            docsForValue = new Set()
            valueHashBucket.set(entry.valueHash, docsForValue)
        }
        docsForValue.add(docId)

        if (entry.numericValue !== null) {
            const numericEntries = cache.fieldNumeric.get(entry.fieldPath) ?? []
            numericEntries.push({ docId, numericValue: entry.numericValue })
            cache.fieldNumeric.set(entry.fieldPath, numericEntries)
        }

        if (entry.valueType === 'string') {
            const stringEntries = cache.fieldString.get(entry.fieldPath) ?? []
            stringEntries.push({ docId, rawValue: entry.rawValue })
            cache.fieldString.set(entry.fieldPath, stringEntries)
        }
    }

    private deleteEntryFromCache(
        cache: CollectionIndexCache,
        docId: _ttid,
        entry: StoredIndexEntry
    ) {
        const valueHashBucket = cache.fieldHash.get(entry.fieldPath)
        const docsForValue = valueHashBucket?.get(entry.valueHash)
        docsForValue?.delete(docId)
        if (docsForValue?.size === 0) valueHashBucket?.delete(entry.valueHash)
        if (valueHashBucket?.size === 0) cache.fieldHash.delete(entry.fieldPath)

        if (entry.numericValue !== null) {
            const numericEntries = cache.fieldNumeric
                .get(entry.fieldPath)
                ?.filter(
                    (candidate) =>
                        !(
                            candidate.docId === docId &&
                            candidate.numericValue === entry.numericValue
                        )
                )
            if (!numericEntries?.length) cache.fieldNumeric.delete(entry.fieldPath)
            else cache.fieldNumeric.set(entry.fieldPath, numericEntries)
        }

        if (entry.valueType === 'string') {
            const stringEntries = cache.fieldString
                .get(entry.fieldPath)
                ?.filter(
                    (candidate) =>
                        !(candidate.docId === docId && candidate.rawValue === entry.rawValue)
                )
            if (!stringEntries?.length) cache.fieldString.delete(entry.fieldPath)
            else cache.fieldString.set(entry.fieldPath, stringEntries)
        }
    }

    private async writeIndexFile(collection: string, cache: CollectionIndexCache) {
        await this.storage.mkdir(this.indexesRoot(collection))
        const target = this.indexFilePath(collection)
        const temp = `${target}.tmp`

        const payload: StoredCollectionIndex = {
            version: 1,
            docs: Object.fromEntries(cache.docs)
        }

        await writeFile(temp, JSON.stringify(payload), 'utf8')
        await rename(temp, target)
    }

    private async loadIndexCache(collection: string) {
        const cache = this.createEmptyIndexCache()

        try {
            const indexPayload = await this.storage.read(this.indexFilePath(collection))
            let raw: StoredCollectionIndex | undefined

            try {
                raw = JSON.parse(indexPayload) as StoredCollectionIndex | undefined
            } catch {
                throw new Error(`Invalid FYLO index file for collection: ${collection}`)
            }

            if (raw?.version === 1 && raw.docs) {
                for (const [docId, entries] of Object.entries(raw.docs) as Array<
                    [_ttid, StoredIndexEntry[]]
                >) {
                    cache.docs.set(docId, entries)
                    for (const entry of entries) this.addEntryToCache(cache, docId, entry)
                }
            }
        } catch (err) {
            if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err
        }

        this.indexes.set(collection, cache)
        return cache
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
        await this.storage.mkdir(this.indexesRoot(collection))
        await this.loadIndexCache(collection)
    }

    private async withCollectionWriteLock<T>(
        collection: string,
        action: () => Promise<T>
    ): Promise<T> {
        const previous = this.writeLanes.get(collection) ?? Promise.resolve()
        let release!: () => void
        const current = new Promise<void>((resolve) => {
            release = resolve
        })
        const lane = previous.then(() => current)
        this.writeLanes.set(collection, lane)

        await previous

        try {
            return await action()
        } finally {
            release()
            if (this.writeLanes.get(collection) === lane) this.writeLanes.delete(collection)
        }
    }

    async createCollection(collection: string) {
        await this.ensureCollection(collection)
    }

    async dropCollection(collection: string) {
        this.indexes.delete(collection)
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
                        return await Cipher.encrypt(String(item).replaceAll('/', '%2F'))
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
                    copy[field] = await Cipher.encrypt(String(fieldValue).replaceAll('/', '%2F'))
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

    private async docResults<T extends Record<string, any>>(
        collection: string,
        query?: _storeQuery<T>
    ) {
        const candidateIds = await this.queryEngine.candidateDocIdsForQuery(collection, query)
        const ids = candidateIds
            ? Array.from(candidateIds)
            : await this.documents.listDocIds(collection)
        const limit = query?.$limit
        const results: Array<FyloRecord<T>> = []

        for (const id of ids) {
            const stored = await this.documents.readStoredDoc<T>(collection, id)
            if (!stored) continue
            if (!this.queryEngine.matchesQuery(id, stored.data, query)) continue
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
        const cache = await this.loadIndexCache(collection)
        const entries = keys.indexes.map((logicalKey) => {
            const segments = logicalKey.split('/')
            const fieldPath = segments.slice(0, -2).join('/')
            const rawValue = segments.at(-2) ?? ''
            const normalized = this.normalizeIndexValue(rawValue)

            return {
                fieldPath,
                rawValue: normalized.rawValue,
                valueHash: normalized.valueHash,
                valueType: normalized.valueType,
                numericValue: normalized.numericValue
            } satisfies StoredIndexEntry
        })

        const existingEntries = cache.docs.get(docId)
        if (existingEntries) {
            for (const entry of existingEntries) this.deleteEntryFromCache(cache, docId, entry)
        }

        cache.docs.set(docId, entries)
        for (const entry of entries) this.addEntryToCache(cache, docId, entry)
        await this.writeIndexFile(collection, cache)
    }

    private async removeIndexes<T extends Record<string, any>>(
        collection: string,
        docId: _ttid,
        _doc: T
    ) {
        const cache = await this.loadIndexCache(collection)
        const existingEntries = cache.docs.get(docId) ?? []
        for (const entry of existingEntries) this.deleteEntryFromCache(cache, docId, entry)
        cache.docs.delete(docId)
        await this.writeIndexFile(collection, cache)
    }

    async putDocument<T extends Record<string, any>>(collection: string, docId: _ttid, doc: T) {
        validateDocId(docId)
        await this.withCollectionWriteLock(collection, async () => {
            const owner = Bun.randomUUIDv7()
            if (!(await this.locks.acquire(collection, docId, owner)))
                throw new Error(`Unable to acquire filesystem lock for ${docId}`)

            const targetPath = this.docPath(collection, docId)

            try {
                await this.documents.writeStoredDoc(collection, docId, doc)
                await this.rebuildIndexes(collection, docId, doc)
                await this.events.publish(collection, {
                    ts: Date.now(),
                    action: 'insert',
                    id: docId,
                    doc: await this.encodeEncrypted(collection, doc)
                })
                await this.runSyncTask(collection, docId, 'put', targetPath, async () => {
                    await this.syncWrite({
                        operation: 'put',
                        collection,
                        docId,
                        path: targetPath,
                        data: doc
                    })
                })
            } finally {
                await this.locks.release(collection, docId, owner)
            }
        })
    }

    async patchDocument<T extends Record<string, any>>(
        collection: string,
        oldId: _ttid,
        newId: _ttid,
        patch: Partial<T>,
        oldDoc?: T
    ) {
        validateDocId(oldId)
        validateDocId(newId)
        return await this.withCollectionWriteLock(collection, async () => {
            const owner = Bun.randomUUIDv7()
            if (!(await this.locks.acquire(collection, oldId, owner)))
                throw new Error(`Unable to acquire filesystem lock for ${oldId}`)

            const oldPath = this.docPath(collection, oldId)

            try {
                const existing =
                    oldDoc ?? (await this.documents.readStoredDoc<T>(collection, oldId))?.data
                if (!existing) return oldId

                const nextDoc = { ...existing, ...patch } as T
                const newPath = this.docPath(collection, newId)
                await this.removeIndexes(collection, oldId, existing)
                await this.documents.removeStoredDoc(collection, oldId)
                await this.events.publish(collection, {
                    ts: Date.now(),
                    action: 'delete',
                    id: oldId,
                    doc: await this.encodeEncrypted(collection, existing)
                })
                await this.documents.writeStoredDoc(collection, newId, nextDoc)
                await this.rebuildIndexes(collection, newId, nextDoc)
                await this.events.publish(collection, {
                    ts: Date.now(),
                    action: 'insert',
                    id: newId,
                    doc: await this.encodeEncrypted(collection, nextDoc)
                })
                await this.runSyncTask(collection, newId, 'patch', newPath, async () => {
                    await this.syncDelete({
                        operation: 'patch',
                        collection,
                        docId: oldId,
                        path: oldPath
                    })
                    await this.syncWrite({
                        operation: 'patch',
                        collection,
                        docId: newId,
                        previousDocId: oldId,
                        path: newPath,
                        data: nextDoc
                    })
                })
                return newId
            } finally {
                await this.locks.release(collection, oldId, owner)
            }
        })
    }

    async deleteDocument<T extends Record<string, any>>(collection: string, docId: _ttid) {
        validateDocId(docId)
        await this.withCollectionWriteLock(collection, async () => {
            const owner = Bun.randomUUIDv7()
            if (!(await this.locks.acquire(collection, docId, owner)))
                throw new Error(`Unable to acquire filesystem lock for ${docId}`)

            const targetPath = this.docPath(collection, docId)

            try {
                const existing = await this.documents.readStoredDoc<T>(collection, docId)
                if (!existing) return
                await this.removeIndexes(collection, docId, existing.data)
                await this.documents.removeStoredDoc(collection, docId)
                await this.events.publish(collection, {
                    ts: Date.now(),
                    action: 'delete',
                    id: docId,
                    doc: await this.encodeEncrypted(collection, existing.data)
                })
                await this.runSyncTask(collection, docId, 'delete', targetPath, async () => {
                    await this.syncDelete({
                        operation: 'delete',
                        collection,
                        docId,
                        path: targetPath
                    })
                })
            } finally {
                await this.locks.release(collection, docId, owner)
            }
        })
    }

    getDoc<T extends Record<string, any>>(
        collection: string,
        docId: _ttid,
        onlyId: boolean = false
    ) {
        validateDocId(docId)
        const engine = this

        return {
            async *[Symbol.asyncIterator]() {
                const doc = await this.once()
                if (Object.keys(doc).length > 0) yield onlyId ? Object.keys(doc).shift()! : doc

                for await (const event of engine.events.listen(collection)) {
                    if (event.action !== 'insert' || event.id !== docId || !event.doc) continue
                    const doc = await engine.decodeEncrypted(collection, event.doc as T)
                    yield onlyId ? event.id : ({ [event.id]: doc } as FyloRecord<T>)
                }
            },
            async once() {
                const stored = await engine.documents.readStoredDoc<T>(collection, docId)
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
                const result = engine.queryEngine.processDoc(doc, query)
                if (result !== undefined) yield result
            }
        }

        return {
            async *[Symbol.asyncIterator]() {
                for await (const result of collectDocs()) yield result

                for await (const event of engine.events.listen(collection)) {
                    if (event.action !== 'insert' || !event.doc) continue
                    const doc = await engine.decodeEncrypted(collection, event.doc as T)
                    if (!engine.queryEngine.matchesQuery(event.id, doc, query)) continue
                    const processed = engine.queryEngine.processDoc(
                        { [event.id]: doc } as FyloRecord<T>,
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
                    const doc = await engine.decodeEncrypted(collection, event.doc as T)
                    if (!engine.queryEngine.matchesQuery(event.id, doc, query)) continue
                    yield event.id
                }
            }
        }
    }

    async *exportBulkData<T extends Record<string, any>>(collection: string) {
        const ids = await this.documents.listDocIds(collection)
        for (const id of ids) {
            const stored = await this.documents.readStoredDoc<T>(collection, id)
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
                        const leftValue = this.queryEngine.getValueByPath(
                            leftData as Record<string, any>,
                            String(field)
                        )
                        const rightValue = this.queryEngine.getValueByPath(
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

                let projected = docs[`${leftId}, ${rightId}`] as Record<string, any>
                if (join.$select?.length) {
                    projected = this.queryEngine.selectValues(
                        join.$select as Array<keyof typeof projected>,
                        projected
                    )
                }
                if (join.$rename) {
                    projected = this.queryEngine.renameFields(
                        join.$rename as Record<string, string>,
                        projected
                    )
                }
                docs[`${leftId}, ${rightId}`] = projected as
                    | T
                    | U
                    | (T & U)
                    | (Partial<T> & Partial<U>)

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
