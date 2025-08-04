import Query from './query'
import Parser from './parser'
import Dir from "./Directory";
import TTID from '@vyckr/ttid';
import Gen from "@vyckr/chex"
import Walker from './Walker';
import S3 from "./S3"
import { $ } from "bun"
import './format'

export default class Sylo {

    private static LOGGING = process.env.LOGGING

    private static MAX_CPUS = navigator.hardwareConcurrency

    private static readonly STRICT = process.env.STRICT

    private dir: Dir;

    constructor() {
        this.dir = new Dir()
    }

    /**
     * Executes a SQL query and returns the results.
     * @param SQL The SQL query to execute.
     * @returns The results of the query.
     */
    async executeSQL<T extends Record<string, any>, U extends Record<string, any> = Record<string, any>>(SQL: string) {
        
        const op = SQL.match(/^(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP)/i)

        if(!op) throw new Error("Missing SQL Operation")

        switch(op.shift()) {
            case "CREATE":
                return await Sylo.createCollection((Parser.parse(SQL) as _storeDelete<T>).$collection!)
            case "DROP":
                return await Sylo.dropCollection((Parser.parse(SQL) as _storeDelete<T>).$collection!)
            case "SELECT":
                const query = Parser.parse<T>(SQL) as _storeQuery<T>
                if(SQL.includes('JOIN')) return await Sylo.joinDocs(query as _join<T, U>)
                const selCol = (query as _storeQuery<T>).$collection
                delete (query as _storeQuery<T>).$collection
                let docs: Record<string, any> | Array<_ttid> = query.$onlyIds ? new Array<_ttid> : {}
                for await (const data of Sylo.findDocs(selCol! as string, query as _storeQuery<T>).collect()) {
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

        await $`aws s3 mb s3://${S3.getBucketFormat(collection)}`.quiet()
    }

    /**
     * Drops an existing schema for a collection.
     * @param collection The name of the collection.
     */
    static async dropCollection(collection: string) {

        await $`aws s3 rm s3://${S3.getBucketFormat(collection)} --recursive`.quiet()

        await $`aws s3 rb s3://${S3.getBucketFormat(collection)}`.quiet()
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

        if(!res.headers.get('content-type')?.includes('application/json')) throw new Error(`Invalid content type: ${res.headers.get('content-type')}`)

        const allData: T[] = []

        let batch = 0
        let count = 0

        let isIncomplete = false
        let incompleteData = ''

        const batchWrite = async () => {

            batch++

            if(limit && count + allData.length > limit) {
                await this.batchPutData(collection, allData.slice(0, limit - count))
                allData.length = 0
                count = limit
                return 
            }

            count += allData.length

            if(count % 10000 === 0) console.log("Count:", count)

            const start = Date.now()
            await this.batchPutData(collection, allData)
            const bytes = allData.toString().length
            const elapsed = Date.now() - start
            const bytesPerSec = (bytes / (elapsed / 1000)).toFixed(2)
            if(Sylo.LOGGING) {
                console.log(`Batch ${batch} of ${JSON.stringify(allData).length} bytes took ${elapsed === Infinity ? 'Infinity' : elapsed}ms (${bytesPerSec} bytes/sec)`)
            }

            allData.length = 0
        }

        const processParquet = async (parquet: string) => {

            const lines = parquet.split('\n')

            for(let line of lines) {

                try {

                    if(isIncomplete) {
                        line = incompleteData + line
                        isIncomplete = false
                    }

                    allData.push(JSON.parse(line))

                    if(allData.length === Sylo.MAX_CPUS) await batchWrite()

                } catch(e) {

                    incompleteData = line
                    isIncomplete = true
                }
            }

            if(allData.length > 0) await batchWrite()
        }

        const clone = res.clone()

        try {

            const data = await res.json()

            if(Array.isArray(data)) {

                let parquetData = ''

                for(const datum of data) parquetData += JSON.stringify(datum) + '\n'

                await processParquet(parquetData)

            } else await processParquet(JSON.stringify(data))
            
        } catch(e) {

            let finished = false

            const reader = clone.body!.getReader()

            do {

                const { done, value } = await reader.read()

                if(done) finished = true

                await processParquet(new TextDecoder('utf-8').decode(value))

            }  while(!finished)
        }

        return count
    }

    /**
     * Exports data from a collection to a URL.
     * @param collection The name of the collection.
     * @returns The current data exported from the collection.
     */
    static async *exportBulkData<T extends Record<string, any>>(collection: string) {

        let token: string | undefined

        do {

            const data = await S3.list(collection, {
                continuationToken: token,
                delimiter: '/'
            })

            if(!data.commonPrefixes) break

            const ids = data.commonPrefixes.map(item => item.prefix!.split('/')[1]!) as _ttid[]

            const res = await Promise.allSettled(ids.map(id => this.getDoc<T>(collection, id).once()))

            const docs = res.filter(item => item.status === 'fulfilled').map(item => item.value)

            for(const doc of docs) {
                for(const id in doc) {
                    yield doc[id]
                }
            }

            token = data.nextContinuationToken

        } while(token !== undefined)
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

        const batches: T[][] = []
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
    async putData<T extends Record<string, any>>(collection: string, data: Record<_ttid, T> | T) {

        const currId = Object.keys(data).shift()!
        
        const _id = TTID.isTTID(currId) ? TTID.generate(currId) : TTID.generate()
        
        let doc = TTID.isTTID(currId) ? Object.values(data).shift() as T : data as T

        if(Sylo.STRICT) doc = await Gen.validateData(collection, doc) as T
        
        const keys = Dir.extractKeys(_id, doc)

        const results = await Promise.allSettled(keys.data.map((item, i) => this.dir.putKeys(collection, { dataKey: item, indexKey: keys.indexes[i] })))

        if(results.some(res => res.status === "rejected")) {
            await this.dir.executeRollback()
            throw new Error(`Unable to write to ${collection} collection`)
        }
        
        if(Sylo.LOGGING) console.log(`Finished Writing ${_id}`)

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
        
        const _id = Object.keys(newDoc).shift() as _ttid

        let _newId = _id
        
        if(!_id) throw new Error("this document does not contain an TTID")

        const keys: string[] = []

        let size = Object.keys(oldDoc).length

        if(size === 0) {

            const items = await Walker.getDocData(collection, _id)

            keys.push(...items)

            const data = await Dir.reconstructData(collection, items)

            oldDoc = { [_id]: data } as Record<_ttid, T>

            size = Object.keys(oldDoc).length
        }

        if(size > 0) {

            const currData = oldDoc[_id]

            const data = newDoc[_id]

            for(const field in data) currData[field] = data[field]!

            await this.delDoc(collection, _id)
            
            _newId = await this.putData(collection, { [_id]: currData })
        }

        if(Sylo.LOGGING) console.log(`Finished Updating ${_id} to ${_newId}`)

        return _newId
    }

    /**
     * Patches documents in a collection.
     * @param collection The name of the collection.
     * @param updateSchema The update schema.
     * @returns The number of documents patched.
     */
    async patchDocs<T extends Record<string, any>>(collection: string, updateSchema: _storeUpdate<T>) {
        
        const processDoc = (doc: Record<_ttid, T>, updateSchema: _storeUpdate<T>) => {

            for(const _id in doc) 
                return this.patchDoc(collection, { [_id]: updateSchema.$set }, doc)

            return
        }

        let count = 0
        
        const promises: Promise<_ttid>[] = []

        let finished = false

        const exprs = Query.getExprs(updateSchema.$where ?? {})

        if(exprs.length === 1 && exprs[0] === `**/*`) {

            for(const doc of await Sylo.allDocs<T>(collection, updateSchema.$where)) {

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
        
        if(Sylo.LOGGING) console.log(`Finished Deleting ${_id}`)
    }

    /**
     * Deletes documents from a collection.
     * @param collection The name of the collection.
     * @param deleteSchema The delete schema.
     * @returns The number of documents deleted.
     */
    async delDocs<T extends Record<string, any>>(collection: string, deleteSchema?: _storeDelete<T>) {
        
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

        const exprs = Query.getExprs(deleteSchema ?? {})

        if(exprs.length === 1 && exprs[0] === `**/*`) {

            for(const doc of await Sylo.allDocs<T>(collection, deleteSchema)) {

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

                const leftCollection = leftSegs.shift()!

                const allVals = new Set<string>()

                for(const rightIdx of rightFieldIndexes) {

                    const rightSegs = rightIdx.split('/')
                    const right_id = rightSegs.pop()! as _ttid
                    const rightVal = rightSegs.pop()!

                    const rightCollection = rightSegs.shift()!

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

                // @ts-ignore
                const grouping = Object.groupBy([data], elem => elem[join.$groupby!])

                for(const group in grouping) {

                    if(groupedDocs[group]) groupedDocs[group][ids] = data
                    else groupedDocs[group] = { [ids]: data }
                }
            }

            if(join.$onlyIds) {

                const groupedIds: Record<T[keyof T] | U[keyof U], _ttid[]> = {} as Record<T[keyof T] | U[keyof U], _ttid[]>

                for(const group in groupedDocs) {
                    const doc = groupedDocs[group]
                    groupedIds[group as T[keyof T] | U[keyof U]] = Object.keys(doc).flat()
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
        
        const docs = await Promise.allSettled(ids.map(id => Sylo.getDoc<T>(collection, id).once()))
        
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

                for(let [_id, data] of Object.entries(doc)) {

                    if(query && query.$select && query.$select.length > 0) {

                        data = this.selectValues<T>(query.$select as Array<keyof T>, data)
                    }

                    if(query && query.$rename) data = this.renameFields<T>(query.$rename, data)

                    doc[_id] = data
                }

                if(query && query.$groupby) {

                    const docGroup: Record<T[keyof T], Record<string, Partial<T>>> = {} as Record<T[keyof T], Record<string, Partial<T>>>

                    for(const [id, data] of Object.entries(doc)) {

                        const groupValue = data[query.$groupby]

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

                                //@ts-ignore
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

                const expression = Query.getExprs(query ?? {})

                if(expression.length === 1 && expression[0] === `**/*`) {
                    for(const doc of await Sylo.allDocs<T>(collection, query)) yield processDoc(doc, query)
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

                    count++

                    yield processDoc(value as Record<_ttid, T>, query)

                } while(!finished)
            },
            
            /**
             * Async iterator for the documents.
             */
            async *collect() {

                const expression = Query.getExprs(query ?? {})

                if(expression.length === 1 && expression[0] === `**/*`) {

                    for(const doc of await Sylo.allDocs<T>(collection, query)) yield processDoc(doc, query)
                
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

                        count++

                        yield processDoc(value as Record<_ttid, T>, query)

                    } while(!finished)
                }
            },

            /**
             * Async iterator (listener) for the document's deletion.
             */
            async *onDelete() {

                let count = 0
                let finished = false

                const iter = Dir.searchDocs<T>(collection, Query.getExprs(query ?? {}), {}, { listen: true, skip: true }, true)

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