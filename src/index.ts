/* eslint-disable @typescript-eslint/explicit-function-return-type */
import { Query } from './core/query'
import { Parser } from './core/parser'
import { Dir } from "./core/directory";
import TTID from '@vyckr/ttid';
import Gen from "@vyckr/chex"
import { Walker } from './core/walker';
import { S3 } from "./adapters/s3"
import { Cipher } from "./adapters/cipher"
import './core/format'
import './core/extensions'

export default class Fylo {

    private static LOGGING = process.env.LOGGING

    private static MAX_CPUS = navigator.hardwareConcurrency

    private static readonly STRICT = process.env.STRICT

    private static ttidLock: Promise<void> = Promise.resolve()

    private static readonly SCHEMA_DIR = process.env.SCHEMA_DIR

    /** Collections whose schema `$encrypted` config has already been loaded. */
    private static readonly loadedEncryption: Set<string> = new Set()

    private dir: Dir;

    constructor() {
        this.dir = new Dir()
    }

    /**
     * Executes a SQL query and returns the results.
     * @param SQL The SQL query to execute.
     * @returns The results of the query.
     */
    async executeSQL<T extends Record<string, any>, U extends Record<string, any> = Record<string, unknown>>(SQL: string) {
        
        const op = SQL.match(/^(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP)/i)

        if(!op) throw new Error("Missing SQL Operation")

        switch(op.shift()) {
            case "CREATE":
                return await Fylo.createCollection((Parser.parse(SQL) as _storeDelete<T>).$collection!)
            case "DROP":
                return await Fylo.dropCollection((Parser.parse(SQL) as _storeDelete<T>).$collection!)
            case "SELECT":
                const query = Parser.parse<T>(SQL) as _storeQuery<T>
                if(SQL.includes('JOIN')) return await Fylo.joinDocs(query as _join<T, U>)
                const selCol = (query as _storeQuery<T>).$collection
                delete (query as _storeQuery<T>).$collection
                let docs: Record<string, unknown> | Array<_ttid> = query.$onlyIds ? new Array<_ttid> : {}
                for await (const data of Fylo.findDocs(selCol! as string, query as _storeQuery<T>).collect()) {
                    if(typeof data === 'object') {
                        docs = Object.appendGroup(docs, data)
                    } else (docs as Array<_ttid>).push(data as _ttid)
                }
                return docs
            case "INSERT":
                const insert = Parser.parse<T>(SQL) as _storeInsert<T>
                const insCol = insert.$collection
                delete insert.$collection
                return await this.putData(insCol!, insert.$values)
            case "UPDATE":
                const update = Parser.parse<T>(SQL) as _storeUpdate<T>
                const updateCol = update.$collection
                delete update.$collection
                return await this.patchDocs(updateCol!, update)
            case "DELETE":
                const del = Parser.parse<T>(SQL) as _storeDelete<T>
                const delCol = del.$collection
                delete del.$collection
                return await this.delDocs(delCol!, del)
            default:
                throw new Error("Invalid Operation")
        }
    }

    /**
     * Creates a new schema for a collection.
     * @param collection The name of the collection.
     */
    static async createCollection(collection: string) {

        await S3.createBucket(collection)
    }

    /**
     * Drops an existing schema for a collection.
     * @param collection The name of the collection.
     */
    static async dropCollection(collection: string) {

        await S3.deleteBucket(collection)
    }

    /**
     * Loads encrypted field config from a collection's JSON schema if not already loaded.
     * Reads the `$encrypted` array from the schema and registers fields with Cipher.
     * Auto-configures the Cipher key from `ENCRYPTION_KEY` env var on first use.
     */
    private static async loadEncryption(collection: string): Promise<void> {
        if (Fylo.loadedEncryption.has(collection)) return
        Fylo.loadedEncryption.add(collection)

        if (!Fylo.SCHEMA_DIR) return

        try {
            const res = await import(`${Fylo.SCHEMA_DIR}/${collection}.json`)
            const schema = res.default as Record<string, unknown>
            const encrypted = schema.$encrypted

            if (Array.isArray(encrypted) && encrypted.length > 0) {
                if (!Cipher.isConfigured()) {
                    const secret = process.env.ENCRYPTION_KEY
                    if (!secret) throw new Error('Schema declares $encrypted fields but ENCRYPTION_KEY env var is not set')
                    if (secret.length < 32) throw new Error('ENCRYPTION_KEY must be at least 32 characters long')
                    await Cipher.configure(secret)
                }
                Cipher.registerFields(collection, encrypted as string[])
            }
        } catch {
            // No schema file found — no encryption for this collection
        }
    }

    /**
     * Rolls back all transcations in current instance
     */
    async rollback() {
        await this.dir.executeRollback()
    }

    /**
     * Imports data from a URL into a collection.
     * @param collection The name of the collection.
     * @param url The URL of the data to import.
     * @param limit The maximum number of documents to import.
     */
    async importBulkData<T extends Record<string, any>>(collection: string, url: URL, limit?: number) {

        const res = await fetch(url)

        if(!res.headers.get('content-type')?.includes('application/json')) throw new Error('Response is not JSON')

        let count = 0
        let batchNum = 0

        const flush = async (batch: T[]) => {

            if(!batch.length) return

            const items = limit && count + batch.length > limit ? batch.slice(0, limit - count) : batch

            batchNum++

            const start = Date.now()
            await this.batchPutData(collection, items)
            count += items.length

            if(count % 10000 === 0) console.log("Count:", count)

            if(Fylo.LOGGING) {
                const bytes = JSON.stringify(items).length
                const elapsed = Date.now() - start
                const bytesPerSec = (bytes / (elapsed / 1000)).toFixed(2)
                console.log(`Batch ${batchNum} of ${bytes} bytes took ${elapsed === Infinity ? 'Infinity' : elapsed}ms (${bytesPerSec} bytes/sec)`)
            }
        }

        // Detect format from the first byte of the body:
        //   0x5b ('[') → JSON array: buffer the full body, then parse and process in slices.
        //   Otherwise  → NDJSON stream: parse incrementally with Bun.JSONL.parseChunk, which
        //                accepts Uint8Array directly (zero-copy for ASCII) and tracks the split-line
        //                remainder internally via the returned `read` offset — no manual incomplete-
        //                line state machine needed.
        let isJsonArray: boolean | null = null
        const jsonArrayChunks: Uint8Array[] = []
        let jsonArrayLength = 0

        let pending = new Uint8Array(0)
        let batch: T[] = []

        for await (const chunk of res.body as AsyncIterable<Uint8Array>) {

            if(isJsonArray === null) isJsonArray = chunk[0] === 0x5b

            if(isJsonArray) {
                jsonArrayChunks.push(chunk)
                jsonArrayLength += chunk.length
                continue
            }

            // Prepend any leftover bytes from the previous iteration (an unterminated line),
            // then parse. `read` points past the last complete line; `pending` holds the rest.
            const merged = new Uint8Array(pending.length + chunk.length)
            merged.set(pending)
            merged.set(chunk, pending.length)

            const { values, read } = Bun.JSONL.parseChunk(merged)
            pending = merged.subarray(read)

            for(const item of values) {
                batch.push(item as T)
                if(batch.length === Fylo.MAX_CPUS) {
                    await flush(batch)
                    batch = []
                    if(limit && count >= limit) return count
                }
            }
        }

        if(isJsonArray) {

            // Reassemble buffered chunks into a single Uint8Array and parse as JSON.
            const body = new Uint8Array(jsonArrayLength)
            let offset = 0
            for(const c of jsonArrayChunks) { body.set(c, offset); offset += c.length }

            const data = JSON.parse(new TextDecoder().decode(body))
            const items: T[] = Array.isArray(data) ? data : [data]

            for(let i = 0; i < items.length; i += Fylo.MAX_CPUS) {
                if(limit && count >= limit) break
                await flush(items.slice(i, i + Fylo.MAX_CPUS))
            }

        } else {

            // Flush the in-progress batch and any final line that had no trailing newline.
            if(pending.length > 0) {
                const { values } = Bun.JSONL.parseChunk(pending)
                for(const item of values) batch.push(item as T)
            }

            if(batch.length > 0) await flush(batch)
        }

        return count
    }

    /**
     * Exports data from a collection to a URL.
     * @param collection The name of the collection.
     * @returns The current data exported from the collection.
     */
    static async *exportBulkData<T extends Record<string, any>>(collection: string) {

        // Kick off the first S3 list immediately so there is no idle time at the start.
        let listPromise: Promise<Bun.S3ListObjectsResponse> | null = S3.list(collection, { delimiter: '/' })

        while(listPromise !== null) {

            const data: Bun.S3ListObjectsResponse = await listPromise

            if(!data.commonPrefixes?.length) break

            const ids = data.commonPrefixes
                .map(item => item.prefix!.split('/')[0]!)
                .filter(key => TTID.isTTID(key)) as _ttid[]

            // Start fetching the next page immediately — before awaiting doc reads —
            // so the S3 list round-trip overlaps with document reconstruction.
            listPromise = data.isTruncated && data.nextContinuationToken
                ? S3.list(collection, { delimiter: '/', continuationToken: data.nextContinuationToken })
                : null

            const results = await Promise.allSettled(ids.map(id => this.getDoc<T>(collection, id).once()))

            for(const result of results) {
                if(result.status === 'fulfilled') {
                    for(const id in result.value) yield result.value[id]
                }
            }
        }
    }

    /**
     * Gets a document from a collection.
     * @param collection The name of the collection.
     * @param _id The ID of the document.
     * @param onlyId Whether to only return the ID of the document.
     * @returns The document or the ID of the document.
     */
    static getDoc<T extends Record<string, any>>(collection: string, _id: _ttid, onlyId: boolean = false) {
        
        return {

            /**
             * Async iterator (listener) for the document.
             */
            async *[Symbol.asyncIterator]() {

                const doc = await this.once()

                if(Object.keys(doc).length > 0) yield doc

                let finished = false

                const iter = Dir.searchDocs<T>(collection, `**/${_id.split('-')[0]}*`, {}, { listen: true, skip: true })

                do {

                    const { value, done } = await iter.next({ count: 0 })

                    if(value === undefined && !done) continue

                    if(done) {
                        finished = true
                        break
                    }

                    const doc = value as Record<_ttid, T>

                    const keys = Object.keys(doc)

                    if(onlyId && keys.length > 0) {
                        yield keys.shift()!
                        continue
                    }
                    else if(keys.length > 0) {
                        yield doc
                        continue
                    }

                } while(!finished)
            },

            /**
             * Gets the document once.
             */
            async once() {

                const items = await Walker.getDocData(collection, _id)

                if(items.length === 0) return {}

                const data = await Dir.reconstructData(collection, items)

                return { [_id]: data } as Record<_ttid, T>
            },

            /**
             * Async iterator (listener) for the document's deletion.
             */
            async *onDelete() {

                let finished = false

                const iter = Dir.searchDocs<T>(collection, `**/${_id.split('-')[0]}*`, {}, { listen: true, skip: true }, true)

                do {

                    const { value, done } = await iter.next({ count: 0 })

                    if(value === undefined && !done) continue

                    if(done) {
                        finished = true
                        break
                    }

                    yield value as _ttid

                } while(!finished)
            }
        }
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

        if(batch.length > navigator.hardwareConcurrency) {

            for(let i = 0; i < batch.length; i += navigator.hardwareConcurrency) {
                batches.push(batch.slice(i, i + navigator.hardwareConcurrency))
            }

        } else batches.push(batch)
        
        for(const batch of batches) {

            const res = await Promise.allSettled(batch.map(data => this.putData(collection, data)))

            for(const _id of res.filter(item => item.status === 'fulfilled').map(item => item.value)) {
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
    private static async uniqueTTID(existingId?: string): Promise<_ttid> {

        // Serialize TTID generation so concurrent callers (e.g. batchPutData)
        // never invoke TTID.generate() at the same sub-millisecond instant.
        let _id!: _ttid
        const prev = Fylo.ttidLock
        Fylo.ttidLock = prev.then(async () => {
            _id = existingId ? TTID.generate(existingId) : TTID.generate()
            // Claim in Redis for cross-process uniqueness (no-op if Redis unavailable)
            if(!(await Dir.claimTTID(_id))) throw new Error('TTID collision — retry')
        })
        await Fylo.ttidLock

        return _id
    }

    async putData<T extends Record<string, any>>(collection: string, data: Record<_ttid, T> | T) {

        await Fylo.loadEncryption(collection)

        const currId = Object.keys(data).shift()!
        
        const _id = TTID.isTTID(currId) ? await Fylo.uniqueTTID(currId) : await Fylo.uniqueTTID()
        
        let doc = TTID.isTTID(currId) ? Object.values(data).shift() as T : data as T

        if(Fylo.STRICT) doc = await Gen.validateData(collection, doc) as T
        
        const keys = await Dir.extractKeys(collection, _id, doc)

        const results = await Promise.allSettled(keys.data.map((item, i) => this.dir.putKeys(collection, { dataKey: item, indexKey: keys.indexes[i] })))

        if(results.some(res => res.status === "rejected")) {
            await this.dir.executeRollback()
            throw new Error(`Unable to write to ${collection} collection`)
        }
        
        if(Fylo.LOGGING) console.log(`Finished Writing ${_id}`)

        return _id
    }

    /**
     * Patches a document in a collection.
     * @param collection The name of the collection.
     * @param newDoc The new document data.
     * @param oldDoc The old document data.
     * @returns The number of documents patched.
     */
    async patchDoc<T extends Record<string, any>>(collection: string, newDoc: Record<_ttid, Partial<T>>, oldDoc: Record<_ttid, T> = {}) {

        await Fylo.loadEncryption(collection)

        const _id = Object.keys(newDoc).shift() as _ttid

        let _newId = _id

        if(!_id) throw new Error("this document does not contain an TTID")

        // Fetch data keys once — needed for deletion and, when oldDoc is absent, reconstruction.
        // Previously, delDoc re-fetched these internally, causing a redundant S3 list call per document.
        const dataKeys = await Walker.getDocData(collection, _id)

        if(dataKeys.length === 0) return _newId

        if(Object.keys(oldDoc).length === 0) {

            const data = await Dir.reconstructData(collection, dataKeys)

            oldDoc = { [_id]: data } as Record<_ttid, T>
        }

        if(Object.keys(oldDoc).length === 0) return _newId

        const currData = { ...oldDoc[_id] }

        for(const field in newDoc[_id]) currData[field] = newDoc[_id][field]!

        // Generate new TTID upfront so that delete and write can proceed in parallel.
        _newId = await Fylo.uniqueTTID(_id)

        let docToWrite: T = currData as T

        if(Fylo.STRICT) docToWrite = await Gen.validateData(collection, currData) as T

        const newKeys = await Dir.extractKeys(collection, _newId, docToWrite)

        const [deleteResults, putResults] = await Promise.all([
            Promise.allSettled(dataKeys.map(key => this.dir.deleteKeys(collection, key))),
            Promise.allSettled(newKeys.data.map((item, i) => this.dir.putKeys(collection, { dataKey: item, indexKey: newKeys.indexes[i] })))
        ])

        if(deleteResults.some(r => r.status === 'rejected') || putResults.some(r => r.status === 'rejected')) {
            await this.dir.executeRollback()
            throw new Error(`Unable to update ${collection} collection`)
        }

        if(Fylo.LOGGING) console.log(`Finished Updating ${_id} to ${_newId}`)

        return _newId
    }

    /**
     * Patches documents in a collection.
     * @param collection The name of the collection.
     * @param updateSchema The update schema.
     * @returns The number of documents patched.
     */
    async patchDocs<T extends Record<string, any>>(collection: string, updateSchema: _storeUpdate<T>) {

        await Fylo.loadEncryption(collection)
        
        const processDoc = (doc: Record<_ttid, T>, updateSchema: _storeUpdate<T>) => {

            for(const _id in doc) 
                return this.patchDoc(collection, { [_id]: updateSchema.$set }, doc)

            return
        }

        let count = 0
        
        const promises: Promise<_ttid>[] = []

        let finished = false

        const exprs = await Query.getExprs(collection, updateSchema.$where ?? {})

        if(exprs.length === 1 && exprs[0] === `**/*`) {

            for(const doc of await Fylo.allDocs<T>(collection, updateSchema.$where)) {

                const promise = processDoc(doc, updateSchema)

                if(promise) {
                    promises.push(promise)
                    count++
                }
            }
        
        } else {

            const iter = Dir.searchDocs<T>(collection, exprs, { updated: updateSchema?.$where?.$updated, created: updateSchema?.$where?.$created }, { listen: false, skip: false })

            do {

                const { value, done } = await iter.next({ count })

                if(value === undefined && !done) continue

                if(done) {
                    finished = true
                    break
                }

                const promise = processDoc(value as Record<_ttid, T>, updateSchema)

                if(promise) {
                    promises.push(promise)
                    count++
                }

            } while(!finished)
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
    async delDoc(collection: string, _id: _ttid) {

        const keys = await Walker.getDocData(collection, _id)

        const results = await Promise.allSettled(keys.map(key => this.dir.deleteKeys(collection, key)))

        if(results.some(res => res.status === "rejected")) {
            await this.dir.executeRollback()
            throw new Error(`Unable to delete from ${collection} collection`)
        }
        
        if(Fylo.LOGGING) console.log(`Finished Deleting ${_id}`)
    }

    /**
     * Deletes documents from a collection.
     * @param collection The name of the collection.
     * @param deleteSchema The delete schema.
     * @returns The number of documents deleted.
     */
    async delDocs<T extends Record<string, any>>(collection: string, deleteSchema?: _storeDelete<T>) {

        await Fylo.loadEncryption(collection)
        
        const processDoc = (doc: Record<_ttid, T>) => {

            for(const _id in doc) {

                if(TTID.isTTID(_id)) {
                    return this.delDoc(collection, _id)
                }
            }

            return
        }

        let count = 0

        const promises: Promise<void>[] = []

        let finished = false

        const exprs = await Query.getExprs(collection, deleteSchema ?? {})

        if(exprs.length === 1 && exprs[0] === `**/*`) {

            for(const doc of await Fylo.allDocs<T>(collection, deleteSchema)) {

                const promise = processDoc(doc)

                if(promise) {
                    promises.push(promise)
                    count++
                }
            }

        } else {

            const iter = Dir.searchDocs<T>(collection, exprs, { updated: deleteSchema?.$updated, created: deleteSchema?.$created }, { listen: false, skip: false })

            do {

                const { value, done } = await iter.next({ count })

                if(value === undefined && !done) continue

                if(done) {
                    finished = true
                    break
                }

                const promise = processDoc(value as Record<_ttid, T>)

                if(promise) {
                    promises.push(promise)
                    count++
                }

            } while(!finished)
        }

        await Promise.all(promises)

        return count
    }

    private static selectValues<T extends Record<string, any>>(selection: Array<keyof T>, data: T) {
        
        for(const field in data) {
            if(!selection.includes(field as keyof T)) delete data[field]
        }

        return data
    }

    private static renameFields<T extends Record<string, any>>(rename: Record<keyof T, string>, data: T) {
        
        for(const field in data) {
            if(rename[field]) {
                data[rename[field]] = data[field]
                delete data[field]
            }
        }

        return data
    }

    /**
     * Joins documents from two collections.
     * @param join The join schema.
     * @returns The joined documents.
     */
    static async joinDocs<T extends Record<string, any>, U extends Record<string, any>>(join: _join<T, U>) { 
        
        const docs: Record<`${_ttid}, ${_ttid}`, T | U | T & U | Partial<T> & Partial<U>> = {}

        const compareFields = async (leftField: keyof T, rightField: keyof U, compare: (leftVal: string, rightVal: string) => boolean) => {

            if(join.$leftCollection === join.$rightCollection) throw new Error("Left and right collections cannot be the same")
            
            let leftToken: string | undefined
            const leftFieldIndexes: string[] = []

            do {

                const leftData = await S3.list(join.$leftCollection, {
                    prefix: String(leftField)
                })
                
                if(!leftData.contents) break

                leftFieldIndexes.push(...leftData.contents!.map(content => content.key!))

                leftToken = leftData.nextContinuationToken

            } while(leftToken !== undefined)
            
            let rightToken: string | undefined
            const rightFieldIndexes: string[] = []

            do { 

                const rightData = await S3.list(join.$rightCollection, {
                    prefix: String(rightField)
                })
                
                if(!rightData.contents) break

                rightFieldIndexes.push(...rightData.contents!.map(content => content.key!))

                rightToken = rightData.nextContinuationToken

            } while(rightToken !== undefined)

            for(const leftIdx of leftFieldIndexes) {

                const leftSegs = leftIdx.split('/')
                const left_id = leftSegs.pop()! as _ttid
                const leftVal = leftSegs.pop()!

                const leftCollection = join.$leftCollection

                const allVals = new Set<string>()

                for(const rightIdx of rightFieldIndexes) {

                    const rightSegs = rightIdx.split('/')
                    const right_id = rightSegs.pop()! as _ttid
                    const rightVal = rightSegs.pop()!

                    const rightCollection = join.$rightCollection

                    if(compare(rightVal, leftVal) && !allVals.has(rightVal)) {

                        allVals.add(rightVal)

                        switch(join.$mode) {
                            case "inner":
                                docs[`${left_id}, ${right_id}`] = { [leftField]: Dir.parseValue(leftVal), [rightField]: Dir.parseValue(rightVal) } as Partial<T> & Partial<U>
                                break
                            case "left":
                                const leftDoc = await this.getDoc<T>(leftCollection, left_id).once()
                                if(Object.keys(leftDoc).length > 0) {
                                    let leftData = leftDoc[left_id]
                                    if(join.$select) leftData = this.selectValues<T>(join.$select as Array<keyof T>, leftData)
                                    if(join.$rename) leftData = this.renameFields<T>(join.$rename, leftData)
                                    docs[`${left_id}, ${right_id}`] = leftData as T
                                }
                                break
                            case "right":
                                const rightDoc = await this.getDoc<U>(rightCollection, right_id).once()
                                if(Object.keys(rightDoc).length > 0) {
                                    let rightData = rightDoc[right_id]
                                    if(join.$select) rightData = this.selectValues<U>(join.$select as Array<keyof U>, rightData)
                                    if(join.$rename) rightData = this.renameFields<U>(join.$rename, rightData)
                                    docs[`${left_id}, ${right_id}`] = rightData as U
                                }
                                break
                            case "outer":

                                let leftFullData: T = {} as T
                                let rightFullData: U = {} as U

                                const leftFullDoc = await this.getDoc<T>(leftCollection, left_id).once()

                                if(Object.keys(leftFullDoc).length > 0) {
                                    let leftData = leftFullDoc[left_id]
                                    if(join.$select) leftData = this.selectValues<T>(join.$select as Array<keyof T>, leftData)
                                    if(join.$rename) leftData = this.renameFields<T>(join.$rename, leftData)
                                    leftFullData = { ...leftData, ...leftFullData } as T
                                }

                                const rightFullDoc = await this.getDoc<U>(rightCollection, right_id).once()

                                if(Object.keys(rightFullDoc).length > 0) {
                                    let rightData = rightFullDoc[right_id]
                                    if(join.$select) rightData = this.selectValues<U>(join.$select as Array<keyof U>, rightData)
                                    if(join.$rename) rightData = this.renameFields<U>(join.$rename, rightData)
                                    rightFullData = { ...rightData, ...rightFullData } as U
                                }

                                docs[`${left_id}, ${right_id}`] = { ...leftFullData, ...rightFullData } as T & U
                                break
                        }

                        if(join.$limit && Object.keys(docs).length === join.$limit) break
                    }
                }

                if(join.$limit && Object.keys(docs).length === join.$limit) break
            }
        }

        for(const field in join.$on) {

            if(join.$on[field]!.$eq) await compareFields(field, join.$on[field]!.$eq, (leftVal, rightVal) => leftVal === rightVal)

            if(join.$on[field]!.$ne) await compareFields(field, join.$on[field]!.$ne, (leftVal, rightVal) => leftVal !== rightVal)
            
            if(join.$on[field]!.$gt) await compareFields(field, join.$on[field]!.$gt, (leftVal, rightVal) => Number(leftVal) > Number(rightVal))
            
            if(join.$on[field]!.$lt) await compareFields(field, join.$on[field]!.$lt, (leftVal, rightVal) => Number(leftVal) < Number(rightVal))
            
            if(join.$on[field]!.$gte) await compareFields(field, join.$on[field]!.$gte, (leftVal, rightVal) => Number(leftVal) >= Number(rightVal))
            
            if(join.$on[field]!.$lte) await compareFields(field, join.$on[field]!.$lte, (leftVal, rightVal) => Number(leftVal) <= Number(rightVal))
        }

        if(join.$groupby) {

            const groupedDocs: Record<string, Record<string, Partial<T | U>>> = {} as Record<string, Record<string, Partial<T | U>>>

            for(const ids in docs) {

                const data = docs[ids as `${_ttid}, ${_ttid}`]

                // @ts-expect-error - Object.groupBy not yet in TS lib types
                const grouping = Object.groupBy([data], elem => elem[join.$groupby!])

                for(const group in grouping) {

                    if(groupedDocs[group]) groupedDocs[group][ids] = data
                    else groupedDocs[group] = { [ids]: data }
                }
            }

            if(join.$onlyIds) {

                const groupedIds: Record<string, _ttid[]> = {}

                for(const group in groupedDocs) {
                    const doc = groupedDocs[group]
                    groupedIds[group] = Object.keys(doc).flat()
                }

                return groupedIds
            }
            
            return groupedDocs
        }

        if(join.$onlyIds) return Array.from(new Set(Object.keys(docs).flat()))    

        return docs
    }

    private static async allDocs<T extends Record<string, any>>(collection: string, query?: _storeQuery<T>) {

        const res = await S3.list(collection, {
            delimiter: '/',
            maxKeys: !query || !query.$limit ? undefined : query.$limit
        })
        
        const ids = res.commonPrefixes?.map(item => item.prefix!.split('/')[0]!).filter(key => TTID.isTTID(key)) as _ttid[] ?? [] as _ttid[]
        
        const docs = await Promise.allSettled(ids.map(id => Fylo.getDoc<T>(collection, id).once()))
        
        return docs.filter(item => item.status === 'fulfilled').map(item => item.value).filter(doc => Object.keys(doc).length > 0)
    }

    /**
     * Finds documents in a collection.
     * @param collection The name of the collection.
     * @param query The query schema.
     * @returns The found documents.
     */
    static findDocs<T extends Record<string, any>>(collection: string, query?: _storeQuery<T>) {
        
        const processDoc = (doc: Record<_ttid, T>, query?: _storeQuery<T>) => {

            if(Object.keys(doc).length > 0) {

                // Post-filter for operators that cannot be expressed as globs ($ne, $gt, $gte, $lt, $lte).
                // $ops use OR semantics: a document passes if it matches at least one op.
                if(query?.$ops) {
                    for(const [_id, data] of Object.entries(doc)) {
                        let matchesAny = false
                        for(const op of query.$ops) {
                            let opMatches = true
                            for(const col in op) {
                                const val = (data as Record<string, unknown>)[col]
                                const cond = op[col as keyof T]!
                                if(cond.$ne !== undefined && val == cond.$ne) { opMatches = false; break }
                                if(cond.$gt !== undefined && !(Number(val) > cond.$gt)) { opMatches = false; break }
                                if(cond.$gte !== undefined && !(Number(val) >= cond.$gte)) { opMatches = false; break }
                                if(cond.$lt !== undefined && !(Number(val) < cond.$lt)) { opMatches = false; break }
                                if(cond.$lte !== undefined && !(Number(val) <= cond.$lte)) { opMatches = false; break }
                            }
                            if(opMatches) { matchesAny = true; break }
                        }
                        if(!matchesAny) delete doc[_id as _ttid]
                    }
                    if(Object.keys(doc).length === 0) return
                }

                for(let [_id, data] of Object.entries(doc)) {

                    if(query && query.$select && query.$select.length > 0) {

                        data = this.selectValues<T>(query.$select as Array<keyof T>, data)
                    }

                    if(query && query.$rename) data = this.renameFields<T>(query.$rename, data)

                    doc[_id] = data
                }

                if(query && query.$groupby) {

                    const docGroup: Record<string, Record<string, Partial<T>>> = {}

                    for(const [id, data] of Object.entries(doc)) {

                        const groupValue = data[query.$groupby] as string

                        if(groupValue) {

                            delete data[query.$groupby]

                            docGroup[groupValue] = {
                                [id]: data as Partial<T>
                            } as Record<_ttid, Partial<T>>
                        }
                    }

                    if(query && query.$onlyIds) {

                        for(const [groupValue, doc] of Object.entries(docGroup)) {

                            for(const id in doc as Record<_ttid, T>) {

                                // @ts-expect-error - dynamic key assignment on grouped object
                                docGroup[groupValue][id] = null
                            }
                        }

                        return docGroup
                    }

                    return docGroup
                }

                if(query && query.$onlyIds) {
                    return Object.keys(doc).shift()
                }

                return doc
            }

            return 
        }

        return {

            /**
             * Async iterator (listener) for the documents.
             */
            async *[Symbol.asyncIterator]() {

                await Fylo.loadEncryption(collection)

                const expression = await Query.getExprs(collection, query ?? {})

                if(expression.length === 1 && expression[0] === `**/*`) {
                    for(const doc of await Fylo.allDocs<T>(collection, query)) yield processDoc(doc, query)
                } 

                let count = 0
                let finished = false

                const iter = Dir.searchDocs<T>(collection, expression, { updated: query?.$updated, created: query?.$created }, { listen: true, skip: true })

                do {

                    const { value, done } = await iter.next({ count, limit: query?.$limit })

                    if(value === undefined && !done) continue

                    if(done) {
                        finished = true
                        break
                    }

                    const result = processDoc(value as Record<_ttid, T>, query)
                    if(result !== undefined) {
                        count++
                        yield result
                    }

                } while(!finished)
            },
            
            /**
             * Async iterator for the documents.
             */
            async *collect() {

                await Fylo.loadEncryption(collection)

                const expression = await Query.getExprs(collection, query ?? {})

                if(expression.length === 1 && expression[0] === `**/*`) {

                    for(const doc of await Fylo.allDocs<T>(collection, query)) yield processDoc(doc, query)
                
                } else {

                    let count = 0
                    let finished = false

                    const iter = Dir.searchDocs<T>(collection, expression, { updated: query?.$updated, created: query?.$created }, { listen: false, skip: false })

                    do {

                        const { value, done } = await iter.next({ count, limit: query?.$limit })

                        if(value === undefined && !done) continue

                        if(done) {
                            finished = true
                            break
                        }

                        const result = processDoc(value as Record<_ttid, T>, query)
                        if(result !== undefined) {
                            count++
                            yield result
                        }

                    } while(!finished)
                }
            },

            /**
             * Async iterator (listener) for the document's deletion.
             */
            async *onDelete() {

                await Fylo.loadEncryption(collection)

                let count = 0
                let finished = false

                const iter = Dir.searchDocs<T>(collection, await Query.getExprs(collection, query ?? {}), {}, { listen: true, skip: true }, true)

                do {

                    const { value, done } = await iter.next({ count })

                    if(value === undefined && !done) continue

                    if(done) {
                        finished = true
                        break
                    }

                    if(value) yield value as _ttid

                } while(!finished)
            }
        }
    }
}