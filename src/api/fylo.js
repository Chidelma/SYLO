import path from 'node:path'
import TTID from '@d31ma/ttid'
import Gen from '@d31ma/chex'
import { Parser } from '../query/parser.js'
import { FyloAuthError } from '../security/auth.js'
import { Cipher } from '../security/cipher.js'
import { FilesystemEngine } from '../storage/engine.js'
import { emitFyloEvent } from '../observability/events.js'
import { validateDocId } from '../core/doc-id.js'
import {
    normalizeImportOptions,
    assertImportUrlAllowed,
    redactImportUrl,
    tlsCheckServerIdentity
} from '../security/import-guard.js'
import '../core/extensions.js'

/**
 * @typedef {import('../security/auth.js').FyloAuthAction} FyloAuthAction
 * @typedef {import('../security/auth.js').FyloAuthContext} FyloAuthContext
 * @typedef {import('../security/auth.js').FyloAuthorizeInput} FyloAuthorizeInput
 * @typedef {Omit<FyloAuthorizeInput, 'auth'>} FyloAuthorizeRequest
 * @typedef {import('../security/auth.js').FyloAuthPolicy} FyloAuthPolicy
 * @typedef {import('../replication/sync.js').FyloOptions<Record<string, any>>} FyloOptions
 * @typedef {import('../replication/sync.js').FyloSyncMode} FyloSyncMode
 * @typedef {import('../replication/sync.js').FyloSyncHooks<Record<string, any>>} FyloSyncHooks
 * @typedef {import('../replication/sync.js').FyloWriteSyncEvent<Record<string, any>>} FyloWriteSyncEvent
 * @typedef {import('../replication/sync.js').FyloDeleteSyncEvent} FyloDeleteSyncEvent
 * @typedef {import('../replication/sync.js').FyloWormMode} FyloWormMode
 * @typedef {import('../replication/sync.js').FyloWormOptions} FyloWormOptions
 * @typedef {import('../replication/sync.js').FyloWormWriteSyncInfo} FyloWormWriteSyncInfo
 * @typedef {import('../replication/sync.js').FyloWormDeleteSyncInfo} FyloWormDeleteSyncInfo
 * @typedef {import('../observability/events.js').FyloEvent} FyloEvent
 * @typedef {import('../observability/events.js').FyloEventHandler} FyloEventHandler
 * @typedef {import('../query/types.js').StoreDelete<Record<string, any>>} StoreDelete
 * @typedef {import('../query/types.js').StoreInsert<Record<string, any>>} StoreInsert
 * @typedef {import('../query/types.js').StoreJoin<Record<string, any>, Record<string, any>>} StoreJoin
 * @typedef {import('../query/types.js').StoreQuery<Record<string, any>>} StoreQuery
 * @typedef {import('../query/types.js').StoreUpdate<Record<string, any>>} StoreUpdate
 * @typedef {import('../types/vendor.js').TTID} TTIDValue
 * @typedef {import('../storage/types.js').CollectionInspectResult} CollectionInspectResult
 * @typedef {import('../storage/types.js').CollectionRebuildResult} CollectionRebuildResult
 * @typedef {import('../types/fylo.js').GetDocResult<Record<string, any>>} GetDocResult
 * @typedef {import('../types/fylo.js').FindDocsResult<Record<string, any>>} FindDocsResult
 * @typedef {import('../types/fylo.js').JoinDocsResult<Record<string, any>, Record<string, any>>} JoinDocsResult
 */

/**
 * @typedef {import('../security/import-guard.js').ImportBulkDataOptions} ImportBulkDataOptions
 */

/**
 * @typedef {object} FyloHistoryEntry
 * @property {TTIDValue} id
 * @property {number} createdAt
 * @property {number} updatedAt
 * @property {Record<string, any>} data
 * @property {TTIDValue} lineageId
 * @property {TTIDValue=} previousVersionId
 * @property {number=} supersededAt
 * @property {boolean} isHead
 * @property {boolean} deleted
 * @property {number=} deletedAt
 */
export default class Fylo {
    /** @type {string | undefined} */
    static LOGGING = process.env.LOGGING
    /** @type {number} */
    static MAX_CPUS = navigator.hardwareConcurrency
    /** @type {string | undefined} */
    static STRICT = process.env.STRICT
    /** @type {Promise<void>} */
    static ttidLock = Promise.resolve()
    /** Collections whose schema `$encrypted` config has already been loaded. */
    /** @type {Set<string>} */
    static loadedEncryption = new Set()
    /** @type {FilesystemEngine} */
    engine
    /** @type {FyloAuthPolicy | undefined} */
    authPolicy
    /** @type {FyloEventHandler | undefined} */
    onEvent
    /**
     * @param {FyloOptions} [options]
     */
    constructor(options = {}) {
        this.authPolicy = options.auth
        this.onEvent = options.onEvent
        this.engine = new FilesystemEngine(options.root ?? Fylo.defaultRoot(), {
            sync: options.sync,
            syncMode: options.syncMode,
            worm: options.worm,
            onEvent: options.onEvent
        })
    }
    /** @returns {string} */
    static defaultRoot() {
        return process.env.FYLO_ROOT ?? path.join(process.cwd(), '.fylo-data')
    }
    /** @returns {FilesystemEngine} */
    static get defaultEngine() {
        return new FilesystemEngine(Fylo.defaultRoot())
    }
    /**
     * Executes a SQL query and returns the results.
     * @param {string} SQL The SQL query to execute.
     * @returns The results of the query.
     */
    async executeSQL(SQL) {
        const op = SQL.match(/^(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP)/i)
        if (!op) throw new Error('Missing SQL Operation')
        switch (op.shift()) {
            case 'CREATE':
                return await this.createCollection(
                    /** @type {{ $collection: string }} */ (Parser.parse(SQL)).$collection
                )
            case 'DROP':
                return await this.dropCollection(
                    /** @type {{ $collection: string }} */ (Parser.parse(SQL)).$collection
                )
            case 'SELECT': {
                const query = /** @type {StoreQuery} */ (Parser.parse(SQL))
                if (SQL.includes('JOIN'))
                    return await this.joinDocs(/** @type {StoreJoin} */ (query))
                const selCol = query.$collection
                delete query.$collection
                /** @type {TTIDValue[] | Record<string, any>} */
                let docs = query.$onlyIds ? [] : {}
                for await (const data of this.findDocs(String(selCol), query).collect()) {
                    if (typeof data === 'object')
                        docs = /** @type {{ appendGroup(target: any, value: any): any }} */ (
                            /** @type {unknown} */ (Object)
                        ).appendGroup(docs, data)
                    else docs.push(data)
                }
                return docs
            }
            case 'INSERT': {
                const insert = /** @type {StoreInsert} */ (Parser.parse(SQL))
                const insCol = insert.$collection
                delete insert.$collection
                return await this.putData(String(insCol), insert.$values)
            }
            case 'UPDATE': {
                const update = /** @type {StoreUpdate} */ (Parser.parse(SQL))
                const updateCol = update.$collection
                delete update.$collection
                return await this.patchDocs(String(updateCol), update)
            }
            case 'DELETE': {
                const del = /** @type {StoreDelete} */ (Parser.parse(SQL))
                const delCol = del.$collection
                delete del.$collection
                return await this.delDocs(String(delCol), del)
            }
            default:
                throw new Error('Invalid Operation')
        }
    }
    /**
     * Creates a new collection on the configured filesystem root.
     * @param {string} collection The name of the collection.
     * @returns {Promise<void>}
     */
    static async createCollection(collection) {
        await Fylo.defaultEngine.createCollection(collection)
    }
    /**
     * Drops an existing collection from the configured filesystem root.
     * @param {string} collection The name of the collection.
     * @returns {Promise<void>}
     */
    static async dropCollection(collection) {
        await Fylo.defaultEngine.dropCollection(collection)
    }
    /** @param {string} collection @returns {Promise<CollectionRebuildResult>} */
    static async rebuildCollection(collection) {
        return await Fylo.defaultEngine.rebuildCollection(collection)
    }
    /** @param {string} collection @returns {Promise<CollectionInspectResult>} */
    static async inspectCollection(collection) {
        return await Fylo.defaultEngine.inspectCollection(collection)
    }
    /** @param {string} collection @returns {Promise<void>} */
    async createCollection(collection) {
        return await this.engine.createCollection(collection)
    }
    /** @param {string} collection @returns {Promise<void>} */
    async dropCollection(collection) {
        return await this.engine.dropCollection(collection)
    }
    /** @param {string} collection @returns {Promise<CollectionRebuildResult>} */
    async rebuildCollection(collection) {
        return await this.engine.rebuildCollection(collection)
    }
    /** @param {string} collection @returns {Promise<CollectionInspectResult>} */
    async inspectCollection(collection) {
        return await this.engine.inspectCollection(collection)
    }
    /** @param {FyloAuthContext} auth @param {FyloAuthPolicy | undefined} [policy] @returns {AuthenticatedFylo} */
    as(auth, policy = this.authPolicy) {
        if (!policy) throw new Error('FYLO auth policy is not configured')
        return new AuthenticatedFylo(this, auth, policy)
    }
    /**
     * Loads encrypted field config from a collection's JSON schema if not already loaded.
     * Reads the `$encrypted` array from the schema and registers fields with Cipher.
     * Auto-configures the Cipher key from `ENCRYPTION_KEY` env var on first use.
     */
    /** @param {string} collection @returns {Promise<void>} */
    static async loadEncryption(collection) {
        if (Fylo.loadedEncryption.has(collection)) return
        const schemaDir = process.env.SCHEMA_DIR
        if (!schemaDir) {
            Fylo.loadedEncryption.add(collection)
            return
        }
        const schemaPath = path.join(schemaDir, `${collection}.json`)
        let schema
        try {
            schema = await Bun.file(schemaPath).json()
        } catch (err) {
            const error = /** @type {NodeJS.ErrnoException} */ (err)
            if (error.code === 'ENOENT') {
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
            Cipher.registerFields(collection, encrypted)
        }
        Fylo.loadedEncryption.add(collection)
    }
    /** @param {string} collection @param {TTIDValue} _id @param {boolean} [onlyId] @returns {GetDocResult} */
    getDoc(collection, _id, onlyId = false) {
        validateDocId(_id)
        return this.engine.getDoc(collection, _id, onlyId)
    }
    /** @param {string} collection @param {TTIDValue} _id @param {boolean} [onlyId] @returns {Promise<Record<TTIDValue, Record<string, any>> | TTIDValue | null>} */
    async getLatest(collection, _id, onlyId = false) {
        validateDocId(_id)
        if (onlyId) return await this.engine.getLatest(collection, _id, true)
        return await this.engine.getLatest(collection, _id)
    }
    /** @param {string} collection @param {TTIDValue} _id @returns {Promise<FyloHistoryEntry[]>} */
    async getHistory(collection, _id) {
        validateDocId(_id)
        return await this.engine.getHistory(collection, _id)
    }
    /** @param {string} collection @param {StoreQuery} query @returns {FindDocsResult} */
    findDocs(collection, query) {
        return this.engine.findDocs(collection, query)
    }
    /** @param {StoreJoin} join @returns {Promise<JoinDocsResult>} */
    async joinDocs(join) {
        return await this.engine.joinDocs(join)
    }
    /** @param {string} collection @returns {AsyncGenerator<Record<string, any>, void, unknown>} */
    async *exportBulkData(collection) {
        yield* this.engine.exportBulkData(collection)
    }
    /** @param {string} collection @param {URL} url @param {number | ImportBulkDataOptions} [limitOrOptions] @returns {Promise<number>} */
    async importBulkData(collection, url, limitOrOptions) {
        const importOptions = normalizeImportOptions(limitOrOptions)
        const limit = importOptions.limit
        if (limit !== undefined && limit <= 0) return 0
        let pin
        try {
            pin = await assertImportUrlAllowed(url, importOptions)
        } catch (err) {
            const message = err instanceof Error ? err.message : String(err)
            /** @type {'protocol' | 'host' | 'private-network'} */
            let reason = 'host'
            if (message.includes('protocol is not allowed')) reason = 'protocol'
            else if (message.includes('host is not allowed')) reason = 'host'
            else if (message.includes('private address')) reason = 'private-network'
            emitFyloEvent(this.onEvent, {
                type: 'import.blocked',
                reason,
                url: redactImportUrl(url),
                detail: message
            })
            throw err
        }
        /** @type {RequestInit & { tls?: { serverName?: string, checkServerIdentity?: Function } }} */
        const fetchInit = { redirect: 'manual' }
        if (pin) {
            fetchInit.headers = { Host: url.host }
            if (url.protocol === 'https:') {
                fetchInit.tls = {
                    serverName: pin.serverName,
                    /** @param {string} _hostname @param {import('node:tls').PeerCertificate} cert */
                    checkServerIdentity: (_hostname, cert) =>
                        tlsCheckServerIdentity(pin.serverName, cert)
                }
            }
        }
        /** @type {URL[]} */
        const fetchTargets = pin ? pin.pinnedUrls : [url]
        /** @type {Response | undefined} */
        let res
        /** @type {unknown} */
        let lastErr
        for (let i = 0; i < fetchTargets.length; i++) {
            try {
                res = await fetch(fetchTargets[i], fetchInit)
                break
            } catch (err) {
                lastErr = err
                if (i === fetchTargets.length - 1) throw err
            }
        }
        if (!res) throw lastErr instanceof Error ? lastErr : new Error('Import request failed')
        if (res.status >= 300 && res.status < 400) {
            const location = res.headers.get('location') ?? 'unknown'
            emitFyloEvent(this.onEvent, {
                type: 'import.blocked',
                reason: 'redirect',
                url: redactImportUrl(url),
                detail: `redirect to ${location}`
            })
            throw new Error(`Import request redirected to ${location}`)
        }
        if (!res.ok) throw new Error(`Import request failed with status ${res.status}`)
        if (!res.headers.get('content-type')?.includes('application/json'))
            throw new Error('Response is not JSON')
        if (!res.body) throw new Error('Response body is empty')
        let count = 0
        let batchNum = 0
        /** @param {Record<string, any>[]} batch @returns {Promise<void>} */
        const flush = async (batch) => {
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
        let isJsonArray = null
        const jsonArrayChunks = []
        let jsonArrayLength = 0
        let pending = new Uint8Array(0)
        /** @type {Record<string, any>[]} */
        let batch = []
        let totalBytes = 0
        for await (const chunk of /** @type {AsyncIterable<Uint8Array>} */ (
            /** @type {unknown} */ (res.body)
        )) {
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
                batch.push(/** @type {Record<string, any>} */ (item))
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
            let data
            try {
                data = JSON.parse(new TextDecoder().decode(body))
            } catch {
                throw new Error('Invalid JSON in import response')
            }
            const items = /** @type {Record<string, any>[]} */ (Array.isArray(data) ? data : [data])
            for (let i = 0; i < items.length; i += Fylo.MAX_CPUS) {
                if (limit !== undefined && count >= limit) break
                await flush(items.slice(i, i + Fylo.MAX_CPUS))
            }
        } else {
            if (pending.length > 0) {
                const { values } = Bun.JSONL.parseChunk(pending)
                for (const item of values) batch.push(/** @type {Record<string, any>} */ (item))
            }
            if (batch.length > 0) await flush(batch)
        }
        return count
    }
    /**
     * Gets an exported stream of documents from a collection.
     */
    /** @param {string} collection @returns {AsyncGenerator<Record<string, any>, void, unknown>} */
    static async *exportBulkData(collection) {
        yield* Fylo.defaultEngine.exportBulkData(collection)
    }
    /**
     * Gets a document from a collection.
     * @param {string} collection The name of the collection.
     * @param {TTIDValue} _id The ID of the document.
     * @param {boolean} onlyId Whether to only return the ID of the document.
     * @returns The document or the ID of the document.
     */
    static getDoc(collection, _id, onlyId = false) {
        validateDocId(_id)
        return Fylo.defaultEngine.getDoc(collection, _id, onlyId)
    }
    /**
     * Puts multiple documents into a collection.
     * @param {string} collection The name of the collection.
     * @param {Record<string, any>[]} batch The documents to put.
     * @returns The IDs of the documents.
     */
    async batchPutData(collection, batch) {
        const batches = []
        const ids = []
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
    /**
     * Puts a document into a collection.
     * @param collection The name of the collection.
     * @param data The document to put.
     * @returns The ID of the document.
     */
    /** @param {TTIDValue | undefined} existingId @returns {Promise<TTIDValue>} */
    static async uniqueTTID(existingId) {
        let _id
        const prev = Fylo.ttidLock
        Fylo.ttidLock = prev.then(async () => {
            _id = existingId ? TTID.generate(existingId) : TTID.generate()
        })
        await Fylo.ttidLock
        return /** @type {TTIDValue} */ (/** @type {unknown} */ (_id))
    }
    /**
     * Loads schema-driven encryption config for a collection and emits
     * `cipher.configured` if this call was the one that flipped the global
     * Cipher from unconfigured to configured.
     * @param {string} collection
     * @returns {Promise<void>}
     */
    async loadEncryptionWithEvent(collection) {
        const before = Cipher.isConfigured()
        await Fylo.loadEncryption(collection)
        if (!before && Cipher.isConfigured()) {
            emitFyloEvent(this.onEvent, { type: 'cipher.configured', collection })
        }
    }
    /** @param {string} collection @param {Record<string, any>} data @returns {Promise<{ _id: TTIDValue, doc: Record<string, any>, previousId?: TTIDValue }>} */
    async prepareInsert(collection, data) {
        await this.loadEncryptionWithEvent(collection)
        const currId = Object.keys(data).shift()
        const hasExistingId = typeof currId === 'string' && TTID.isTTID(currId)
        const _id = hasExistingId ? await Fylo.uniqueTTID(currId) : await Fylo.uniqueTTID(undefined)
        let doc = hasExistingId ? Object.values(data).shift() : data
        if (Fylo.STRICT) doc = await Gen.validateData(collection, doc)
        return { _id, doc, previousId: hasExistingId ? currId : undefined }
    }
    /** @param {string} collection @param {TTIDValue} _id @param {Record<string, any>} doc @param {TTIDValue | undefined} previousId @returns {Promise<TTIDValue>} */
    async executePutDataDirect(collection, _id, doc, previousId) {
        if (previousId) await this.engine.replaceDocumentVersion(collection, previousId, _id, doc)
        else await this.engine.putDocument(collection, _id, doc)
        if (Fylo.LOGGING) console.log(`Finished Writing ${_id}`)
        return _id
    }
    /** @param {string} collection @param {Record<TTIDValue, Record<string, any>>} newDoc @param {Record<TTIDValue, Record<string, any>>} [oldDoc] @returns {Promise<TTIDValue>} */
    async executePatchDocDirect(collection, newDoc, oldDoc = {}) {
        await this.loadEncryptionWithEvent(collection)
        const _id = Object.keys(newDoc).shift()
        if (!_id) throw new Error('this document does not contain an TTID')
        validateDocId(_id)
        let existingDoc = oldDoc[_id]
        if (!existingDoc) {
            const existing = await this.engine.getDoc(collection, _id).once()
            existingDoc = existing[_id]
        }
        if (!existingDoc) return _id
        const currData = { ...existingDoc, ...newDoc[_id] }
        let docToWrite = currData
        const _newId = await Fylo.uniqueTTID(_id)
        if (Fylo.STRICT) docToWrite = await Gen.validateData(collection, currData)
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
    /** @param {string} collection @param {TTIDValue} _id @returns {Promise<void>} */
    async executeDelDocDirect(collection, _id) {
        validateDocId(_id)
        await this.engine.deleteDocument(collection, _id)
        if (Fylo.LOGGING) console.log(`Finished Deleting ${_id}`)
    }
    /** @param {string} collection @param {Record<string, any>} data @returns {Promise<TTIDValue>} */
    async putData(collection, data) {
        const { _id, doc, previousId } = await this.prepareInsert(collection, data)
        await this.executePutDataDirect(collection, _id, doc, previousId)
        return _id
    }
    /**
     * Patches a document in a collection.
     * @param {string} collection The name of the collection.
     * @param {Record<TTIDValue, Record<string, any>>} newDoc The new document data.
     * @param {Record<TTIDValue, Record<string, any>>} oldDoc The old document data.
     * @returns The number of documents patched.
     */
    async patchDoc(collection, newDoc, oldDoc = {}) {
        return await this.executePatchDocDirect(collection, newDoc, oldDoc)
    }
    /**
     * Patches documents in a collection.
     * @param {string} collection The name of the collection.
     * @param {StoreUpdate} updateSchema The update schema.
     * @returns The number of documents patched.
     */
    async patchDocs(collection, updateSchema) {
        await this.loadEncryptionWithEvent(collection)
        let count = 0
        const promises = []
        for await (const value of this.findDocs(collection, updateSchema.$where ?? {}).collect()) {
            if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                const [_id, current] = Object.entries(value)[0] ?? []
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
    /** @param {string} collection @param {TTIDValue} _id @returns {Promise<void>} */
    async delDoc(collection, _id) {
        await this.executeDelDocDirect(collection, _id)
    }
    /**
     * Deletes documents from a collection.
     * @param collection The name of the collection.
     * @param deleteSchema The delete schema.
     * @returns The number of documents deleted.
     */
    /** @param {string} collection @param {StoreDelete} deleteSchema @returns {Promise<number>} */
    async delDocs(collection, deleteSchema) {
        await this.loadEncryptionWithEvent(collection)
        let count = 0
        const promises = []
        for await (const value of this.findDocs(collection, deleteSchema).collect()) {
            if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                const _id = Object.keys(value).find((docId) => TTID.isTTID(docId))
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
    /** @param {StoreJoin} join @returns {Promise<JoinDocsResult>} */
    static async joinDocs(join) {
        return await Fylo.defaultEngine.joinDocs(join)
    }
    /**
     * Finds documents in a collection.
     * @param collection The name of the collection.
     * @param query The query schema.
     * @returns The found documents.
     */
    /** @param {string} collection @param {StoreQuery} query @returns {FindDocsResult} */
    static findDocs(collection, query) {
        return Fylo.defaultEngine.findDocs(collection, query)
    }
}
export class AuthenticatedFylo {
    /** @type {Fylo} */
    fylo
    /** @type {FyloAuthContext} */
    auth
    /** @type {FyloAuthPolicy} */
    policy
    /**
     * @param {Fylo} fylo
     * @param {FyloAuthContext} auth
     * @param {FyloAuthPolicy} policy
     */
    constructor(fylo, auth, policy) {
        this.fylo = fylo
        this.auth = auth
        this.policy = policy
    }
    /** @param {FyloAuthorizeRequest} input @returns {Promise<void>} */
    async authorize(input) {
        /** @type {FyloAuthorizeInput} */
        const args = { auth: this.auth, ...input }
        if (!(await this.policy.authorize(args))) throw new FyloAuthError(args)
    }
    /** @param {Record<string, any>} data @returns {TTIDValue | undefined} */
    firstDocId(data) {
        return Object.keys(data).find((key) => TTID.isTTID(key))
    }
    /** @param {string} collection @returns {Promise<void>} */
    async createCollection(collection) {
        await this.authorize({ action: 'collection:create', collection })
        return await this.fylo.createCollection(collection)
    }
    /** @param {string} collection @returns {Promise<void>} */
    async dropCollection(collection) {
        await this.authorize({ action: 'collection:drop', collection })
        return await this.fylo.dropCollection(collection)
    }
    /** @param {string} collection @returns {Promise<CollectionRebuildResult>} */
    async rebuildCollection(collection) {
        await this.authorize({ action: 'collection:rebuild', collection })
        return await this.fylo.rebuildCollection(collection)
    }
    /** @param {string} collection @returns {Promise<CollectionInspectResult>} */
    async inspectCollection(collection) {
        await this.authorize({ action: 'collection:inspect', collection })
        return await this.fylo.inspectCollection(collection)
    }
    /** @param {string} collection @param {TTIDValue} _id @param {boolean} [onlyId] @returns {GetDocResult} */
    getDoc(collection, _id, onlyId = false) {
        validateDocId(_id)
        const authorize = this.authorize.bind(this)
        const source = this.fylo.getDoc(collection, _id, onlyId)
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
    /** @param {string} collection @param {TTIDValue} _id @param {boolean} [onlyId] @returns {Promise<Record<TTIDValue, Record<string, any>> | TTIDValue | null>} */
    async getLatest(collection, _id, onlyId = false) {
        await this.authorize({ action: 'doc:read', collection, docId: _id })
        if (onlyId) return await this.fylo.getLatest(collection, _id, true)
        return await this.fylo.getLatest(collection, _id)
    }
    /** @param {string} collection @param {TTIDValue} _id @returns {Promise<FyloHistoryEntry[]>} */
    async getHistory(collection, _id) {
        await this.authorize({ action: 'doc:read', collection, docId: _id })
        return await this.fylo.getHistory(collection, _id)
    }
    /** @param {string} collection @param {StoreQuery} query @returns {FindDocsResult} */
    findDocs(collection, query) {
        const authorize = this.authorize.bind(this)
        const source = this.fylo.findDocs(collection, query)
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
    /** @param {StoreJoin} join @returns {Promise<JoinDocsResult>} */
    async joinDocs(join) {
        await this.authorize({
            action: 'join:execute',
            collections: [join.$leftCollection, join.$rightCollection],
            query: join
        })
        return await this.fylo.joinDocs(join)
    }
    /** @param {string} collection @returns {AsyncGenerator<Record<string, any>, void, unknown>} */
    async *exportBulkData(collection) {
        await this.authorize({ action: 'bulk:export', collection })
        yield* this.fylo.exportBulkData(collection)
    }
    /** @param {string} collection @param {URL} url @param {number | ImportBulkDataOptions} [limitOrOptions] @returns {Promise<number>} */
    async importBulkData(collection, url, limitOrOptions) {
        await this.authorize({
            action: 'bulk:import',
            collection,
            data: { url: url.toString(), options: limitOrOptions }
        })
        if (typeof limitOrOptions === 'number')
            return await this.fylo.importBulkData(collection, url, limitOrOptions)
        return await this.fylo.importBulkData(collection, url, limitOrOptions)
    }
    /** @param {string} SQL @returns {ReturnType<Fylo['executeSQL']>} */
    async executeSQL(SQL) {
        await this.authorize({ action: 'sql:execute', sql: SQL })
        return await this.fylo.executeSQL(SQL)
    }
    /** @param {string} collection @param {Record<string, any>[]} batch @returns {Promise<TTIDValue[]>} */
    async batchPutData(collection, batch) {
        await this.authorize({ action: 'doc:create', collection, data: batch })
        return await this.fylo.batchPutData(collection, batch)
    }
    /** @param {string} collection @param {Record<string, any>} data @returns {Promise<TTIDValue>} */
    async putData(collection, data) {
        await this.authorize({
            action: 'doc:create',
            collection,
            docId: this.firstDocId(data),
            data
        })
        return await this.fylo.putData(collection, data)
    }
    /** @param {string} collection @param {Record<TTIDValue, Record<string, any>>} newDoc @param {Record<TTIDValue, Record<string, any>>} [oldDoc] @returns {Promise<TTIDValue>} */
    async patchDoc(collection, newDoc, oldDoc = {}) {
        await this.authorize({
            action: 'doc:update',
            collection,
            docId: this.firstDocId(newDoc),
            data: newDoc
        })
        return await this.fylo.patchDoc(collection, newDoc, oldDoc)
    }
    /** @param {string} collection @param {StoreUpdate} updateSchema @returns {Promise<number>} */
    async patchDocs(collection, updateSchema) {
        await this.authorize({
            action: 'doc:update',
            collection,
            query: updateSchema.$where,
            data: updateSchema.$set
        })
        return await this.fylo.patchDocs(collection, updateSchema)
    }
    /** @param {string} collection @param {TTIDValue} _id @returns {Promise<void>} */
    async delDoc(collection, _id) {
        await this.authorize({ action: 'doc:delete', collection, docId: _id })
        return await this.fylo.delDoc(collection, _id)
    }
    /** @param {string} collection @param {StoreDelete} deleteSchema @returns {Promise<number>} */
    async delDocs(collection, deleteSchema) {
        await this.authorize({ action: 'doc:delete', collection, query: deleteSchema })
        return await this.fylo.delDocs(collection, deleteSchema)
    }
}
