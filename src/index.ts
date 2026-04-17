/* eslint-disable @typescript-eslint/explicit-function-return-type */
import { lookup } from 'node:dns/promises'
import { readFile } from 'node:fs/promises'
import { isIP } from 'node:net'
import path from 'node:path'
import { Parser } from './core/parser'
import TTID from '@delma/ttid'
import Gen from '@delma/chex'
import { FyloAuthError } from './auth'
import { Cipher } from './adapters/cipher'
import { S3FilesEngine } from './engines/s3-files'
import { validateDocId } from './core/doc-id'
import type { FyloAuthAction, FyloAuthContext, FyloAuthorizeInput, FyloAuthPolicy } from './auth'
import type { FyloOptions } from './sync'
import './core/format'
import './core/extensions'

export { FyloAuthError } from './auth'
export type { FyloAuthAction, FyloAuthContext, FyloAuthorizeInput, FyloAuthPolicy } from './auth'
export { FyloSyncError } from './sync'
export type {
    FyloDeleteSyncEvent,
    FyloOptions,
    FyloSyncHooks,
    FyloSyncMode,
    FyloWriteSyncEvent
} from './sync'

export type ImportBulkDataOptions = {
    limit?: number
    maxBytes?: number
    allowedProtocols?: string[]
    allowedHosts?: string[]
    allowPrivateNetwork?: boolean
}

type NormalizedImportBulkDataOptions = {
    limit?: number
    maxBytes: number
    allowedProtocols: string[]
    allowedHosts?: string[]
    allowPrivateNetwork: boolean
}

export default class Fylo {
    private static LOGGING = process.env.LOGGING

    private static MAX_CPUS = navigator.hardwareConcurrency

    private static readonly STRICT = process.env.STRICT

    private static ttidLock: Promise<void> = Promise.resolve()

    private static readonly DEFAULT_IMPORT_MAX_BYTES = 50 * 1024 * 1024

    /** Collections whose schema `$encrypted` config has already been loaded. */
    private static readonly loadedEncryption: Set<string> = new Set()

    private readonly engine: S3FilesEngine
    private readonly authPolicy?: FyloAuthPolicy

    constructor(options: FyloOptions = {}) {
        this.authPolicy = options.auth
        this.engine = new S3FilesEngine(options.root ?? options.s3FilesRoot ?? Fylo.defaultRoot(), {
            sync: options.sync,
            syncMode: options.syncMode
        })
    }

    private static defaultRoot() {
        return (
            process.env.FYLO_ROOT ??
            process.env.FYLO_S3FILES_ROOT ??
            path.join(process.cwd(), '.fylo-data')
        )
    }

    private static get defaultEngine() {
        return new S3FilesEngine(Fylo.defaultRoot())
    }

    /**
     * Executes a SQL query and returns the results.
     * @param SQL The SQL query to execute.
     * @returns The results of the query.
     */
    async executeSQL<
        T extends Record<string, any>,
        U extends Record<string, any> = Record<string, unknown>
    >(SQL: string) {
        const op = SQL.match(/^(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP)/i)

        if (!op) throw new Error('Missing SQL Operation')

        switch (op.shift()) {
            case 'CREATE':
                return await this.createCollection(
                    (Parser.parse(SQL) as _storeDelete<T>).$collection!
                )
            case 'DROP':
                return await this.dropCollection(
                    (Parser.parse(SQL) as _storeDelete<T>).$collection!
                )
            case 'SELECT': {
                const query = Parser.parse<T>(SQL) as _storeQuery<T>
                if (SQL.includes('JOIN')) return await this.joinDocs(query as _join<T, U>)
                const selCol = query.$collection
                delete query.$collection
                let docs: Record<string, unknown> | Array<_ttid> = query.$onlyIds ? [] : {}

                for await (const data of this.findDocs(selCol! as string, query).collect()) {
                    if (typeof data === 'object') docs = Object.appendGroup(docs, data)
                    else (docs as Array<_ttid>).push(data as _ttid)
                }

                return docs
            }
            case 'INSERT': {
                const insert = Parser.parse<T>(SQL) as _storeInsert<T>
                const insCol = insert.$collection
                delete insert.$collection
                return await this.putData(insCol!, insert.$values)
            }
            case 'UPDATE': {
                const update = Parser.parse<T>(SQL) as _storeUpdate<T>
                const updateCol = update.$collection
                delete update.$collection
                return await this.patchDocs(updateCol!, update)
            }
            case 'DELETE': {
                const del = Parser.parse<T>(SQL) as _storeDelete<T>
                const delCol = del.$collection
                delete del.$collection
                return await this.delDocs(delCol!, del)
            }
            default:
                throw new Error('Invalid Operation')
        }
    }

    /**
     * Creates a new collection on the configured filesystem root.
     * @param collection The name of the collection.
     */
    static async createCollection(collection: string) {
        await Fylo.defaultEngine.createCollection(collection)
    }

    /**
     * Drops an existing collection from the configured filesystem root.
     * @param collection The name of the collection.
     */
    static async dropCollection(collection: string) {
        await Fylo.defaultEngine.dropCollection(collection)
    }

    async createCollection(collection: string) {
        return await this.engine.createCollection(collection)
    }

    async dropCollection(collection: string) {
        return await this.engine.dropCollection(collection)
    }

    as(auth: FyloAuthContext, policy: FyloAuthPolicy | undefined = this.authPolicy) {
        if (!policy) throw new Error('FYLO auth policy is not configured')
        return new AuthenticatedFylo(this, auth, policy)
    }

    /**
     * Loads encrypted field config from a collection's JSON schema if not already loaded.
     * Reads the `$encrypted` array from the schema and registers fields with Cipher.
     * Auto-configures the Cipher key from `ENCRYPTION_KEY` env var on first use.
     */
    private static async loadEncryption(collection: string): Promise<void> {
        if (Fylo.loadedEncryption.has(collection)) return

        const schemaDir = process.env.SCHEMA_DIR

        if (!schemaDir) {
            Fylo.loadedEncryption.add(collection)
            return
        }

        const schemaPath = path.join(schemaDir, `${collection}.json`)
        let schema: Record<string, unknown>

        try {
            schema = JSON.parse(await readFile(schemaPath, 'utf8')) as Record<string, unknown>
        } catch (err) {
            if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
                Fylo.loadedEncryption.add(collection)
                return
            }
            throw err
        }

        const encrypted = schema.$encrypted
        if (encrypted !== undefined && !Array.isArray(encrypted))
            throw new Error(`Schema $encrypted for ${collection} must be an array of field names`)

        if (Array.isArray(encrypted) && encrypted.length > 0) {
            if (!encrypted.every((field) => typeof field === 'string' && field.length > 0))
                throw new Error(`Schema $encrypted for ${collection} must only contain strings`)

            if (!Cipher.isConfigured()) {
                const secret = process.env.ENCRYPTION_KEY
                if (!secret)
                    throw new Error(
                        'Schema declares $encrypted fields but ENCRYPTION_KEY env var is not set'
                    )
                if (secret.length < 32)
                    throw new Error('ENCRYPTION_KEY must be at least 32 characters long')
                await Cipher.configure(secret)
            }
            Cipher.registerFields(collection, encrypted as string[])
        }

        Fylo.loadedEncryption.add(collection)
    }

    /**
     * Compatibility helper. FYLO now writes synchronously to the filesystem,
     * so there is no queued transactional rollback path to execute.
     */
    async rollback() {}

    getDoc<T extends Record<string, any>>(collection: string, _id: _ttid, onlyId: boolean = false) {
        validateDocId(_id)
        return this.engine.getDoc<T>(collection, _id, onlyId)
    }

    findDocs<T extends Record<string, any>>(collection: string, query?: _storeQuery<T>) {
        return this.engine.findDocs<T>(collection, query)
    }

    async joinDocs<T extends Record<string, any>, U extends Record<string, any>>(
        join: _join<T, U>
    ) {
        return await this.engine.joinDocs(join)
    }

    async *exportBulkData<T extends Record<string, any>>(collection: string) {
        yield* this.engine.exportBulkData<T>(collection)
    }

    private unsupportedLegacyApi(feature: string): never {
        throw new Error(
            `${feature} was removed. FYLO now writes synchronously to the filesystem and expects external sync tooling for cloud replication.`
        )
    }

    async getJobStatus(_jobId: string) {
        return this.unsupportedLegacyApi('getJobStatus')
    }

    async getDocStatus(_collection: string, _docId: _ttid) {
        return this.unsupportedLegacyApi('getDocStatus')
    }

    async getDeadLetters(_count: number = 10) {
        return this.unsupportedLegacyApi('getDeadLetters')
    }

    async getQueueStats() {
        return this.unsupportedLegacyApi('getQueueStats')
    }

    async replayDeadLetter(_streamId: string) {
        return this.unsupportedLegacyApi('replayDeadLetter')
    }

    async processQueuedWrites(_count: number = 1, _recover: boolean = false) {
        return this.unsupportedLegacyApi('processQueuedWrites')
    }

    /**
     * Imports data from a URL into a collection.
     * @param collection The name of the collection.
     * @param url The URL of the data to import.
     * @param limit The maximum number of documents to import.
     */
    private static normalizeImportOptions(
        limitOrOptions?: number | ImportBulkDataOptions
    ): NormalizedImportBulkDataOptions {
        const options =
            typeof limitOrOptions === 'number' ? { limit: limitOrOptions } : limitOrOptions

        return {
            limit: options?.limit,
            maxBytes: options?.maxBytes ?? Fylo.DEFAULT_IMPORT_MAX_BYTES,
            allowedProtocols: options?.allowedProtocols ?? ['https:', 'http:', 'data:'],
            allowedHosts: options?.allowedHosts,
            allowPrivateNetwork: options?.allowPrivateNetwork ?? false
        }
    }

    private static isPrivateAddress(address: string): boolean {
        const normalized = address
            .toLowerCase()
            .replace(/^\[|\]$/g, '')
            .split('%')[0]
        const ipv4 = normalized.startsWith('::ffff:')
            ? normalized.slice('::ffff:'.length)
            : normalized

        if (isIP(ipv4) === 4) {
            const [first = 0, second = 0] = ipv4.split('.').map((part) => Number(part))
            return (
                first === 0 ||
                first === 10 ||
                first === 127 ||
                (first === 169 && second === 254) ||
                (first === 172 && second >= 16 && second <= 31) ||
                (first === 192 && second === 168) ||
                (first === 100 && second >= 64 && second <= 127)
            )
        }

        if (isIP(normalized) === 6) {
            if (normalized === '::1' || normalized === '::') return true

            const firstSegment = Number.parseInt(normalized.split(':')[0] || '0', 16)
            return (firstSegment & 0xfe00) === 0xfc00 || (firstSegment & 0xffc0) === 0xfe80
        }

        return false
    }

    private static hostAllowed(hostname: string, allowedHosts?: string[]): boolean {
        if (!allowedHosts?.length) return true

        const host = hostname.toLowerCase()
        return allowedHosts.some((allowed) => {
            const candidate = allowed.toLowerCase()
            return host === candidate || host.endsWith(`.${candidate}`)
        })
    }

    private static async assertImportUrlAllowed(
        url: URL,
        options: NormalizedImportBulkDataOptions
    ) {
        if (!options.allowedProtocols.includes(url.protocol))
            throw new Error(`Import URL protocol is not allowed: ${url.protocol}`)

        if (url.protocol !== 'http:' && url.protocol !== 'https:') return

        if (!Fylo.hostAllowed(url.hostname, options.allowedHosts))
            throw new Error(`Import URL host is not allowed: ${url.hostname}`)

        if (options.allowPrivateNetwork) return

        const hostname = url.hostname.toLowerCase()
        if (hostname === 'localhost' || hostname.endsWith('.localhost'))
            throw new Error(`Import URL resolves to a private address: ${url.hostname}`)

        const addresses =
            isIP(hostname) === 0
                ? (await lookup(hostname, { all: true })).map((result) => result.address)
                : [hostname]

        if (addresses.some((address) => Fylo.isPrivateAddress(address)))
            throw new Error(`Import URL resolves to a private address: ${url.hostname}`)
    }

    async importBulkData<T extends Record<string, any>>(
        collection: string,
        url: URL,
        limit?: number
    ): Promise<number>
    async importBulkData<T extends Record<string, any>>(
        collection: string,
        url: URL,
        options?: ImportBulkDataOptions
    ): Promise<number>
    async importBulkData<T extends Record<string, any>>(
        collection: string,
        url: URL,
        limitOrOptions?: number | ImportBulkDataOptions
    ) {
        const importOptions = Fylo.normalizeImportOptions(limitOrOptions)
        const limit = importOptions.limit

        if (limit !== undefined && limit <= 0) return 0
        await Fylo.assertImportUrlAllowed(url, importOptions)

        const res = await fetch(url)

        if (!res.ok) throw new Error(`Import request failed with status ${res.status}`)
        if (!res.headers.get('content-type')?.includes('application/json'))
            throw new Error('Response is not JSON')
        if (!res.body) throw new Error('Response body is empty')

        let count = 0
        let batchNum = 0

        const flush = async (batch: T[]) => {
            if (!batch.length) return

            const items =
                limit !== undefined && count + batch.length > limit
                    ? batch.slice(0, limit - count)
                    : batch

            if (!items.length) return

            batchNum++

            const start = Date.now()
            await this.batchPutData(collection, items)
            count += items.length

            if (count % 10000 === 0) console.log('Count:', count)

            if (Fylo.LOGGING) {
                const bytes = JSON.stringify(items).length
                const elapsed = Date.now() - start
                const bytesPerSec = (bytes / (elapsed / 1000)).toFixed(2)
                console.log(
                    `Batch ${batchNum} of ${bytes} bytes took ${elapsed === Infinity ? 'Infinity' : elapsed}ms (${bytesPerSec} bytes/sec)`
                )
            }
        }

        let isJsonArray: boolean | null = null
        const jsonArrayChunks: Uint8Array[] = []
        let jsonArrayLength = 0

        let pending = new Uint8Array(0)
        let batch: T[] = []
        let totalBytes = 0

        for await (const chunk of res.body as unknown as AsyncIterable<Uint8Array>) {
            totalBytes += chunk.length
            if (totalBytes > importOptions.maxBytes)
                throw new Error(`Import response exceeded ${importOptions.maxBytes} bytes`)

            if (isJsonArray === null) isJsonArray = chunk[0] === 0x5b

            if (isJsonArray) {
                jsonArrayChunks.push(chunk)
                jsonArrayLength += chunk.length
                continue
            }

            const merged = new Uint8Array(pending.length + chunk.length)
            merged.set(pending)
            merged.set(chunk, pending.length)

            const { values, read } = Bun.JSONL.parseChunk(merged)
            pending = merged.subarray(read)

            for (const item of values) {
                batch.push(item as T)
                if (batch.length === Fylo.MAX_CPUS) {
                    await flush(batch)
                    batch = []
                    if (limit !== undefined && count >= limit) return count
                }
            }
        }

        if (isJsonArray) {
            const body = new Uint8Array(jsonArrayLength)
            let offset = 0
            for (const c of jsonArrayChunks) {
                body.set(c, offset)
                offset += c.length
            }

            let data: unknown
            try {
                data = JSON.parse(new TextDecoder().decode(body))
            } catch {
                throw new Error('Invalid JSON in import response')
            }
            const items: T[] = Array.isArray(data) ? data : [data]

            for (let i = 0; i < items.length; i += Fylo.MAX_CPUS) {
                if (limit !== undefined && count >= limit) break
                await flush(items.slice(i, i + Fylo.MAX_CPUS))
            }
        } else {
            if (pending.length > 0) {
                const { values } = Bun.JSONL.parseChunk(pending)
                for (const item of values) batch.push(item as T)
            }

            if (batch.length > 0) await flush(batch)
        }

        return count
    }

    /**
     * Gets an exported stream of documents from a collection.
     */
    static async *exportBulkData<T extends Record<string, any>>(collection: string) {
        yield* Fylo.defaultEngine.exportBulkData<T>(collection)
    }

    /**
     * Gets a document from a collection.
     * @param collection The name of the collection.
     * @param _id The ID of the document.
     * @param onlyId Whether to only return the ID of the document.
     * @returns The document or the ID of the document.
     */
    static getDoc<T extends Record<string, any>>(
        collection: string,
        _id: _ttid,
        onlyId: boolean = false
    ) {
        validateDocId(_id)
        return Fylo.defaultEngine.getDoc<T>(collection, _id, onlyId)
    }

    /**
     * Puts multiple documents into a collection.
     * @param collection The name of the collection.
     * @param batch The documents to put.
     * @returns The IDs of the documents.
     */
    async batchPutData<T extends Record<string, any>>(collection: string, batch: Array<T>) {
        const batches: Array<Array<T>> = []
        const ids: _ttid[] = []

        if (batch.length > navigator.hardwareConcurrency) {
            for (let i = 0; i < batch.length; i += navigator.hardwareConcurrency) {
                batches.push(batch.slice(i, i + navigator.hardwareConcurrency))
            }
        } else batches.push(batch)

        for (const itemBatch of batches) {
            const res = await Promise.allSettled(
                itemBatch.map((data) => this.putData(collection, data))
            )

            for (const _id of res
                .filter((item) => item.status === 'fulfilled')
                .map((item) => item.value)) {
                ids.push(_id)
            }
        }

        return ids
    }

    async queuePutData<T extends Record<string, any>>(
        _collection: string,
        _data: Record<_ttid, T> | T
    ) {
        return this.unsupportedLegacyApi('queuePutData')
    }

    async queuePatchDoc<T extends Record<string, any>>(
        _collection: string,
        _newDoc: Record<_ttid, Partial<T>>,
        _oldDoc: Record<_ttid, T> = {}
    ) {
        return this.unsupportedLegacyApi('queuePatchDoc')
    }

    async queueDelDoc(_collection: string, _id: _ttid) {
        return this.unsupportedLegacyApi('queueDelDoc')
    }

    /**
     * Puts a document into a collection.
     * @param collection The name of the collection.
     * @param data The document to put.
     * @returns The ID of the document.
     */
    private static async uniqueTTID(existingId?: string): Promise<_ttid> {
        let _id!: _ttid
        const prev = Fylo.ttidLock
        Fylo.ttidLock = prev.then(async () => {
            _id = existingId ? TTID.generate(existingId) : TTID.generate()
        })
        await Fylo.ttidLock

        return _id
    }

    private async prepareInsert<T extends Record<string, any>>(
        collection: string,
        data: Record<_ttid, T> | T
    ) {
        await Fylo.loadEncryption(collection)

        const currId = Object.keys(data).shift()!
        const _id = TTID.isTTID(currId)
            ? await Fylo.uniqueTTID(currId)
            : await Fylo.uniqueTTID(undefined)

        let doc = TTID.isTTID(currId) ? (Object.values(data).shift() as T) : (data as T)

        if (Fylo.STRICT) doc = (await Gen.validateData(collection, doc)) as T

        return { _id, doc }
    }

    private async executePutDataDirect<T extends Record<string, any>>(
        collection: string,
        _id: _ttid,
        doc: T
    ) {
        await this.engine.putDocument(collection, _id, doc)

        if (Fylo.LOGGING) console.log(`Finished Writing ${_id}`)

        return _id
    }

    private async executePatchDocDirect<T extends Record<string, any>>(
        collection: string,
        newDoc: Record<_ttid, Partial<T>>,
        oldDoc: Record<_ttid, T> = {}
    ) {
        await Fylo.loadEncryption(collection)

        const _id = Object.keys(newDoc).shift() as _ttid

        if (!_id) throw new Error('this document does not contain an TTID')
        validateDocId(_id)

        let existingDoc = oldDoc[_id]
        if (!existingDoc) {
            const existing = await this.engine.getDoc<T>(collection, _id).once()
            existingDoc = existing[_id]
        }
        if (!existingDoc) return _id

        const currData = { ...existingDoc, ...newDoc[_id] } as T
        let docToWrite: T = currData
        const _newId = await Fylo.uniqueTTID(_id)
        if (Fylo.STRICT) docToWrite = (await Gen.validateData(collection, currData)) as T

        const nextId = await this.engine.patchDocument(
            collection,
            _id,
            _newId,
            docToWrite,
            existingDoc
        )

        if (Fylo.LOGGING) console.log(`Finished Updating ${_id} to ${nextId}`)

        return nextId
    }

    private async executeDelDocDirect(collection: string, _id: _ttid) {
        validateDocId(_id)
        await this.engine.deleteDocument(collection, _id)

        if (Fylo.LOGGING) console.log(`Finished Deleting ${_id}`)
    }

    async putData<T extends Record<string, any>>(collection: string, data: T): Promise<_ttid>
    async putData<T extends Record<string, any>>(
        collection: string,
        data: Record<_ttid, T>
    ): Promise<_ttid>
    async putData<T extends Record<string, any>>(
        collection: string,
        data: Record<_ttid, T> | T,
        options?: { wait?: boolean; timeoutMs?: number }
    ): Promise<_ttid>
    async putData<T extends Record<string, any>>(
        collection: string,
        data: Record<_ttid, T> | T,
        options: { wait?: boolean; timeoutMs?: number } = {}
    ): Promise<_ttid> {
        if (options.wait === false) {
            this.unsupportedLegacyApi('putData(..., { wait: false })')
        }

        const { _id, doc } = await this.prepareInsert(collection, data)
        await this.executePutDataDirect(collection, _id, doc)
        return _id
    }

    /**
     * Patches a document in a collection.
     * @param collection The name of the collection.
     * @param newDoc The new document data.
     * @param oldDoc The old document data.
     * @returns The number of documents patched.
     */
    async patchDoc<T extends Record<string, any>>(
        collection: string,
        newDoc: Record<_ttid, Partial<T>>,
        oldDoc: Record<_ttid, T> = {},
        options: { wait?: boolean; timeoutMs?: number } = {}
    ): Promise<_ttid> {
        if (options.wait === false) {
            this.unsupportedLegacyApi('patchDoc(..., { wait: false })')
        }

        return await this.executePatchDocDirect(collection, newDoc, oldDoc)
    }

    /**
     * Patches documents in a collection.
     * @param collection The name of the collection.
     * @param updateSchema The update schema.
     * @returns The number of documents patched.
     */
    async patchDocs<T extends Record<string, any>>(
        collection: string,
        updateSchema: _storeUpdate<T>
    ) {
        await Fylo.loadEncryption(collection)

        let count = 0
        const promises: Promise<_ttid>[] = []

        for await (const value of this.findDocs<T>(collection, updateSchema.$where).collect()) {
            if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                const [_id, current] = Object.entries(value as Record<_ttid, T>)[0] ?? []
                if (_id && current) {
                    promises.push(
                        this.patchDoc(collection, { [_id]: updateSchema.$set }, { [_id]: current })
                    )
                    count++
                }
            }
        }

        await Promise.all(promises)

        return count
    }

    /**
     * Deletes a document from a collection.
     * @param collection The name of the collection.
     * @param _id The ID of the document.
     * @returns The number of documents deleted.
     */
    async delDoc(
        collection: string,
        _id: _ttid,
        options: { wait?: boolean; timeoutMs?: number } = {}
    ): Promise<void> {
        if (options.wait === false) {
            this.unsupportedLegacyApi('delDoc(..., { wait: false })')
        }

        await this.executeDelDocDirect(collection, _id)
    }

    /**
     * Deletes documents from a collection.
     * @param collection The name of the collection.
     * @param deleteSchema The delete schema.
     * @returns The number of documents deleted.
     */
    async delDocs<T extends Record<string, any>>(
        collection: string,
        deleteSchema?: _storeDelete<T>
    ) {
        await Fylo.loadEncryption(collection)

        let count = 0
        const promises: Promise<void>[] = []

        for await (const value of this.findDocs<T>(collection, deleteSchema).collect()) {
            if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                const _id = Object.keys(value as Record<_ttid, T>).find((docId) =>
                    TTID.isTTID(docId)
                )
                if (_id) {
                    promises.push(this.delDoc(collection, _id))
                    count++
                }
            }
        }

        await Promise.all(promises)

        return count
    }

    /**
     * Joins documents from two collections.
     * @param join The join schema.
     * @returns The joined documents.
     */
    static async joinDocs<T extends Record<string, any>, U extends Record<string, any>>(
        join: _join<T, U>
    ) {
        return await Fylo.defaultEngine.joinDocs(join)
    }

    /**
     * Finds documents in a collection.
     * @param collection The name of the collection.
     * @param query The query schema.
     * @returns The found documents.
     */
    static findDocs<T extends Record<string, any>>(collection: string, query?: _storeQuery<T>) {
        return Fylo.defaultEngine.findDocs<T>(collection, query)
    }
}

export class AuthenticatedFylo {
    constructor(
        private readonly fylo: Fylo,
        private readonly auth: FyloAuthContext,
        private readonly policy: FyloAuthPolicy
    ) {}

    private async authorize(input: Omit<FyloAuthorizeInput, 'auth'>) {
        const args = { auth: this.auth, ...input }
        if (!(await this.policy.authorize(args))) throw new FyloAuthError(args)
    }

    private firstDocId(data: Record<string, unknown>) {
        return Object.keys(data).find((key) => TTID.isTTID(key))
    }

    async rollback() {
        return await this.fylo.rollback()
    }

    async createCollection(collection: string) {
        await this.authorize({ action: 'collection:create', collection })
        return await this.fylo.createCollection(collection)
    }

    async dropCollection(collection: string) {
        await this.authorize({ action: 'collection:drop', collection })
        return await this.fylo.dropCollection(collection)
    }

    getDoc<T extends Record<string, any>>(collection: string, _id: _ttid, onlyId: boolean = false) {
        validateDocId(_id)
        const authorize = this.authorize.bind(this)
        const source = this.fylo.getDoc<T>(collection, _id, onlyId)

        return {
            async *[Symbol.asyncIterator]() {
                await authorize({ action: 'doc:read', collection, docId: _id })
                yield* source
            },
            async once() {
                await authorize({ action: 'doc:read', collection, docId: _id })
                return await source.once()
            },
            async *onDelete() {
                await authorize({ action: 'doc:read', collection, docId: _id })
                yield* source.onDelete()
            }
        }
    }

    findDocs<T extends Record<string, any>>(collection: string, query?: _storeQuery<T>) {
        const authorize = this.authorize.bind(this)
        const source = this.fylo.findDocs<T>(collection, query)

        return {
            async *[Symbol.asyncIterator]() {
                await authorize({ action: 'doc:find', collection, query })
                yield* source
            },
            async *collect() {
                await authorize({ action: 'doc:find', collection, query })
                yield* source.collect()
            },
            async *onDelete() {
                await authorize({ action: 'doc:find', collection, query })
                yield* source.onDelete()
            }
        }
    }

    async joinDocs<T extends Record<string, any>, U extends Record<string, any>>(
        join: _join<T, U>
    ) {
        await this.authorize({
            action: 'join:execute',
            collections: [join.$leftCollection, join.$rightCollection],
            query: join
        })
        return await this.fylo.joinDocs(join)
    }

    async *exportBulkData<T extends Record<string, any>>(collection: string) {
        await this.authorize({ action: 'bulk:export', collection })
        yield* this.fylo.exportBulkData<T>(collection)
    }

    async importBulkData<T extends Record<string, any>>(
        collection: string,
        url: URL,
        limit?: number
    ): Promise<number>
    async importBulkData<T extends Record<string, any>>(
        collection: string,
        url: URL,
        options?: ImportBulkDataOptions
    ): Promise<number>
    async importBulkData<T extends Record<string, any>>(
        collection: string,
        url: URL,
        limitOrOptions?: number | ImportBulkDataOptions
    ) {
        await this.authorize({
            action: 'bulk:import',
            collection,
            data: { url: url.toString(), options: limitOrOptions }
        })
        if (typeof limitOrOptions === 'number')
            return await this.fylo.importBulkData<T>(collection, url, limitOrOptions)
        return await this.fylo.importBulkData<T>(collection, url, limitOrOptions)
    }

    async executeSQL<
        T extends Record<string, any>,
        U extends Record<string, any> = Record<string, unknown>
    >(SQL: string) {
        await this.authorize({ action: 'sql:execute', sql: SQL })
        return await this.fylo.executeSQL<T, U>(SQL)
    }

    async batchPutData<T extends Record<string, any>>(collection: string, batch: Array<T>) {
        await this.authorize({ action: 'doc:create', collection, data: batch })
        return await this.fylo.batchPutData(collection, batch)
    }

    async putData<T extends Record<string, any>>(collection: string, data: T): Promise<_ttid>
    async putData<T extends Record<string, any>>(
        collection: string,
        data: Record<_ttid, T>
    ): Promise<_ttid>
    async putData<T extends Record<string, any>>(
        collection: string,
        data: Record<_ttid, T> | T,
        options?: { wait?: boolean; timeoutMs?: number }
    ): Promise<_ttid>
    async putData<T extends Record<string, any>>(
        collection: string,
        data: Record<_ttid, T> | T,
        options: { wait?: boolean; timeoutMs?: number } = {}
    ): Promise<_ttid> {
        await this.authorize({
            action: 'doc:create',
            collection,
            docId: this.firstDocId(data),
            data
        })
        return await this.fylo.putData(collection, data, options)
    }

    async patchDoc<T extends Record<string, any>>(
        collection: string,
        newDoc: Record<_ttid, Partial<T>>,
        oldDoc: Record<_ttid, T> = {},
        options: { wait?: boolean; timeoutMs?: number } = {}
    ): Promise<_ttid> {
        await this.authorize({
            action: 'doc:update',
            collection,
            docId: this.firstDocId(newDoc),
            data: newDoc
        })
        return await this.fylo.patchDoc(collection, newDoc, oldDoc, options)
    }

    async patchDocs<T extends Record<string, any>>(
        collection: string,
        updateSchema: _storeUpdate<T>
    ) {
        await this.authorize({
            action: 'doc:update',
            collection,
            query: updateSchema.$where,
            data: updateSchema.$set
        })
        return await this.fylo.patchDocs(collection, updateSchema)
    }

    async delDoc(
        collection: string,
        _id: _ttid,
        options: { wait?: boolean; timeoutMs?: number } = {}
    ): Promise<void> {
        await this.authorize({ action: 'doc:delete', collection, docId: _id })
        return await this.fylo.delDoc(collection, _id, options)
    }

    async delDocs<T extends Record<string, any>>(
        collection: string,
        deleteSchema?: _storeDelete<T>
    ) {
        await this.authorize({ action: 'doc:delete', collection, query: deleteSchema })
        return await this.fylo.delDocs(collection, deleteSchema)
    }
}
