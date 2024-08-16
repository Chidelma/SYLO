import Query from './Kweeree'
import Paser from './Paza'
import Dir from "./Directory";
import ULID from './ULID';
import Walker from './Walker';
import { rmdir, mkdir, readdir, rm } from 'node:fs/promises';

export default class Stawrij {

    private static SCHEMA = (process.env.SCHEMA || 'STRICT') as _schema

    private static LOGGING = process.env.LOGGING === 'true'

    private static MAX_CPUS = navigator.hardwareConcurrency

    static async executeSQL<T extends Record<string, any>, U extends Record<string, any> = {}>(SQL: string) {

        const op = SQL.match(/^(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|USE)/i)

        if(!op) throw new Error("Missing SQL Operation")

        switch(op[0]) {
            case "USE":
                return Paser.convertUse(SQL)
            case "CREATE":
                return await Stawrij.createSchema(Paser.convertTableCRUD(SQL).collection!)
            case "ALTER":   
                return await Stawrij.modifySchema(Paser.convertTableCRUD(SQL).collection!)
            case "DROP":
                return Stawrij.dropSchema(Paser.convertTableCRUD(SQL).collection!)
            case "SELECT":
                const query = Paser.convertSelect<T>(SQL)
                if(SQL.includes('JOIN')) return await Stawrij.joinDocs(query as _join<T, U>)
                const selCol = (query as _storeQuery<T>).$collection
                delete (query as _storeQuery<T>).$collection
                const docs = new Map<_ulid, T>()
                for await (const data of Stawrij.findDocs(selCol! as string, query as _storeQuery<T>).collect()) {
                    const doc = data as Map<_ulid, T>
                    for(let [_id, data] of doc) {
                        docs.set(_id, data)
                    }
                }
                return docs
            case "INSERT":
                const insert = Paser.convertInsert<T>(SQL)
                const insCol = insert.$collection
                delete insert.$collection
                return await Stawrij.putData(insCol!, insert.$values)
            case "UPDATE":
                const update = Paser.convertUpdate<T>(SQL)
                const updateCol = update.$collection
                delete update.$collection
                return await Stawrij.patchDocs(updateCol!, update)
            case "DELETE":
                const del = Paser.convertDelete<T>(SQL)
                const delCol = del.$collection
                delete del.$collection
                return await Stawrij.delDocs(delCol!, del)
            default:
                throw new Error("Invalid Operation")
        }
    }

    static async createSchema(collection: string) {
        try {
            await Dir.createSchema(collection)
        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.createSchema -> ${e.message}`)
        }
    }

    static async modifySchema(collection: string) {
        try {
            await Dir.modifySchema(collection)
        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.modifySchema -> ${e.message}`)
        }
    }

    static async dropSchema(collection: string) {
        try {
            await Dir.dropSchema(collection)
        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.dropSchema -> ${e.message}`)
        }
    }

    static async importBulkData<T extends Record<string, any>>(collection: string, url: URL, limit?: number) {

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
                await Stawrij.batchPutData(collection, allData.slice(0, limit - count))
                allData.length = 0
                count = limit
                return 
            }

            count += allData.length

            if(count % 8000 === 0) console.log("Count:", count)

            const start = Date.now()
            await Stawrij.batchPutData(collection, allData)
            const bytes = allData.toString().length
            const elapsed = Date.now() - start
            const bytesPerSec = (bytes / (elapsed / 1000)).toFixed(2)
            if(this.LOGGING) {
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

                    if(allData.length === Stawrij.MAX_CPUS) await batchWrite()

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

    static async *exportBulkData<T extends Record<string, any>>(collection: string, url: URL) {

        // extract the file extension from the URL
        const fileExtension = url.pathname.split('.').pop()!

        // check if the file extension is valid
        if (fileExtension !== 'json') throw new Error(`Invalid file extension: ${fileExtension}`)

        const writer = Bun.file(url).writer()

        for await (const data of this.findDocs<T>(collection).collect()) {

            const doc = data as Map<_ulid, T>

            for(let [_id, data] of doc) {

                yield _id

                writer.write(JSON.stringify(data) + '\n')
            }
        }

        await writer.end()

        await Stawrij.dropSchema(collection)
    }

    static getDoc<T extends Record<string, any>>(collection: string, _id: _ulid, onlyId: boolean = false) {

        return {

            async *[Symbol.asyncIterator]() {

                for await (const data of Dir.searchDocs<T>(`${collection}/**/${_id}`, {}, true)) {

                    const doc = data as Map<_ulid, T>

                    if(onlyId && doc.size > 0) {
                        await Bun.sleep(100)
                        yield _id
                        continue
                    }
                    else if(doc.size > 0) {
                        await Bun.sleep(100)
                        yield doc
                        continue
                    }
                }
            },

            async once() {

                const indexes = await Walker.getDocIndexes(collection, _id)

                if(indexes.length === 0) return new Map<_ulid, T>()

                const data = await Dir.reconstructData<T>(indexes)

                return new Map<_ulid, T>([[_id, data]])
            },

            async *onDelete() {
                for await (const _ of Dir.searchDocs<T>(`${collection}/**/${_id}`, {}, true, true)) {
                    yield _id
                }
            }
        }
    }

    static async batchPutData<T extends Record<string, any>>(collection: string, batch: Array<T>) {

        if(batch.length > navigator.hardwareConcurrency * 2) throw new Error("Batch size must be less than or equal to the number of CPUs")
        
        await Promise.allSettled(batch.map(data => Stawrij.putData(collection, data)))
    }

    static async putData<T extends Record<string, any>>(collection: string, data: Map<_ulid, T> | T, indexes?: string[]) {

        const _id = data instanceof Map ? Array.from((data as Map<_ulid, T>).keys())[0] : ULID.generate()
        
        try {

            if(this.SCHEMA === 'STRICT' && !collection.startsWith('_')) await Dir.validateData(collection, data)
            
            await Dir.aquireLock(collection, _id)

            indexes ??= await Walker.getDocIndexes(collection, _id)

            await Promise.allSettled(indexes.map(idx => Dir.deleteIndex(idx)))

            const files = await readdir(`${Walker.DSK_DB}/${collection}/${Walker.DIRECT_DIR}/${_id}`, { withFileTypes: true })
            
            await Promise.allSettled(files.filter(file => !file.isSymbolicLink()).map(file => rm(`${Walker.DSK_DB}/${collection}/${Walker.DIRECT_DIR}/${_id}/${file.name}`, { recursive: true })))
            
            const doc = data instanceof Map ? (data as Map<_ulid, T>).get(_id)! : data as T

            await mkdir(`${Walker.DSK_DB}/${collection}/${Walker.DIRECT_DIR}/${_id}`, { recursive: true })

            await Promise.allSettled(Dir.deconstructData(collection, _id, doc).map(idx => Dir.putIndex(idx)))

            if(this.LOGGING) console.log(`Finished Writing ${_id}`)
            
            await Dir.releaseLock(collection, _id)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.putData -> ${e.message}`)
        }

        return _id
    }

    static async patchDoc<T extends Record<string, any>>(collection: string, newDoc: Map<_ulid, Partial<T>>, oldDoc: Map<_ulid, T> = new Map<_ulid, T>()) {
        
        try {

            const _id = Array.from(newDoc.keys())[0] as _ulid

            if(!_id) throw new Error("Stawrij document does not contain an UUID")

            const indexes: string[] = []

            if(oldDoc.size === 0) {

                indexes.push(...await Walker.getDocIndexes(collection, _id))

                const data = await Dir.reconstructData<T>(indexes)

                oldDoc = new Map([[_id, data]]) as Map<_ulid, T>
            }

            if(oldDoc.size > 0) {

                const currData = oldDoc.get(_id)!

                const data = newDoc.get(_id)!

                for(const field in data) currData[field] = data[field]!

                await this.putData(collection, new Map([[_id, currData]]) as Map<_ulid, T>, indexes)
            }

            if(this.LOGGING) console.log(`Finished Updating ${_id}`)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.patchDoc -> ${e.message}`)
        }
    }


    static async patchDocs<T extends Record<string, any>>(collection: string, updateSchema: _storeUpdate<T>) {

        let count = 0
        
        try {

            const promises: Promise<void>[] = []

            for await (const data of Dir.searchDocs<T>(Query.getExprs(updateSchema.$where ?? {}, collection), { updated: updateSchema?.$where?.$updated, created: updateSchema?.$where?.$created })) {

                const doc = data as Map<_ulid, T>

                if(doc.size > 0) {

                    for(const [_id] of doc) promises.push(Stawrij.patchDoc(collection, new Map([[_id, updateSchema.$set]]), doc))

                    count++
                }
            }

            await Promise.allSettled(promises)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.putDoc -> ${e.message}`)
        }

        return count
    }

    static async delDoc(collection: string, _id: _ulid) {

        try {

            await Dir.aquireLock(collection, _id)

            const indexes = await Walker.getDocIndexes(collection, _id)

            await Promise.allSettled(indexes.map(idx => Dir.deleteIndex(idx)))

            await rmdir(`${Walker.DSK_DB}/${collection}/${Walker.DIRECT_DIR}/${_id}`, { recursive: true })

            if(this.LOGGING) console.log(`Finished Deleting ${_id}`)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.delDoc -> ${e.message}`)
        }
    }

    static async delDocs<T extends Record<string, any>>(collection: string, deleteSchema?: _storeDelete<T>) {

        let count = 0

        try {

            const promises: Promise<void>[] = []

            for await (const data of Dir.searchDocs<T>(Query.getExprs(deleteSchema ?? {}, collection), { updated: deleteSchema?.$updated, created: deleteSchema?.$created })) {
            
                const doc = data as Map<_ulid, T>

                if(doc.size > 0) {
 
                    for(const [_id] of doc) promises.push(Stawrij.delDoc(collection, _id))

                    count++
                }
            }

            await Promise.allSettled(promises)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.delDocs -> ${e.message}`)
        }

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

    static async joinDocs<T extends Record<string, any>, U extends Record<string, any>>(join: _join<T, U>) { 
        
        const docs: Map<_ulid[], T | U | T & U | Partial<T> & Partial<U>> = new Map<_ulid[], T | U | T & U | Partial<T> & Partial<U>>()

        try {

            const compareFields = async (leftField: keyof T, rightField: keyof U, compare: (leftVal: string, rightVal: string) => boolean) => {

                try {

                    if(join.$leftCollection === join.$rightCollection) throw new Error("Left and right collections cannot be the same")

                    const leftFieldIndexes = Array.from(new Bun.Glob(`${join.$leftCollection}/${String(leftField)}/**`).scanSync({ cwd: Walker.DSK_DB })) 
                    const rightFieldIndexes = Array.from(new Bun.Glob(`${join.$rightCollection}/${String(rightField)}/**`).scanSync({ cwd: Walker.DSK_DB }))

                    for(const leftIdx of leftFieldIndexes) {
        
                        const leftSegs = leftIdx.split('/')
                        const left_id = leftSegs.pop()! as _ulid
                        const leftVal = leftSegs.pop()!
        
                        const leftCollection = leftSegs.shift()!
        
                        const allVals = new Set<string>()
        
                        for(const rightIdx of rightFieldIndexes) {
        
                            const rightSegs = rightIdx.split('/')
                            const right_id = rightSegs.pop()! as _ulid
                            const rightVal = rightSegs.pop()!
        
                            const rightCollection = rightSegs.shift()!
        
                            if(compare(rightVal, leftVal) && !allVals.has(rightVal)) {
        
                                allVals.add(rightVal)
        
                                switch(join.$mode) {
                                    case "inner":
                                        docs.set([left_id, right_id], { [leftField]: Dir.parseValue(leftVal), [rightField]: Dir.parseValue(rightVal) } as Partial<T> & Partial<U>)
                                        break
                                    case "left":
                                        for await (const data of Dir.searchDocs<T>(`${leftCollection}/**/${left_id}`, {})) {
                                            const leftDoc = data as Map<_ulid, T>
                                            let leftData = leftDoc.get(left_id)!
                                            if(join.$select) leftData = this.selectValues<T>(join.$select as Array<keyof T>, leftData)
                                            if(join.$rename) leftData = this.renameFields<T>(join.$rename, leftData)
                                            docs.set([left_id, right_id], leftData as T)
                                        }
                                        break
                                    case "right":
                                        for await (const data of Dir.searchDocs<U>(`${rightCollection}/**/${right_id}`, {})) {
                                            const rightDoc = data as Map<_ulid, U>
                                            let rightData = rightDoc.get(right_id)!
                                            if(join.$select) rightData = this.selectValues<U>(join.$select as Array<keyof U>, rightData)
                                            if(join.$rename) rightData = this.renameFields<U>(join.$rename, rightData)
                                            docs.set([left_id, right_id], rightData as U)
                                        }
                                        break
                                    case "outer":

                                        let leftFullData: T = {} as T
                                        let rightFullData: U = {} as U

                                        for await (const data of Dir.searchDocs<T>(`${leftCollection}/**/${left_id}`, {})) {
                                            const leftDoc = data as Map<_ulid, T>
                                            let leftData = leftDoc.get(left_id)!
                                            if(join.$select) leftData = this.selectValues<T>(join.$select as Array<keyof T>, leftData)
                                            if(join.$rename) leftData = this.renameFields<T>(join.$rename, leftData)
                                            leftFullData = { ...leftData, ...leftFullData } as T
                                        }

                                        for await (const data of Dir.searchDocs<U>(`${rightCollection}/**/${right_id}`, {})) {
                                            const rightDoc = data as Map<_ulid, U>
                                            let rightData = rightDoc.get(right_id)!
                                            if(join.$select) rightData = this.selectValues<U>(join.$select as Array<keyof U>, rightData)
                                            if(join.$rename) rightData = this.renameFields<U>(join.$rename, rightData)
                                            rightFullData = { ...rightData, ...rightFullData } as U
                                        }

                                        docs.set([left_id, right_id], { ...leftFullData, ...rightFullData } as T & U)
                                        break
                                }
        
                                if(join.$limit && docs.size === join.$limit) break
                            }
                        }
        
                        if(join.$limit && docs.size === join.$limit) break
                    }

                } catch(e) {
                    if(e instanceof Error) throw new Error(`Stawrij.joinDocs.compareFields -> ${e.message}`)
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
    
                const groupedDocs: Map<T[keyof T] | U[keyof U], Map<_ulid[], Partial<T | U>>> = new Map<T[keyof T] | U[keyof U], Map<_ulid[], Partial<T | U>>>()
    
                for(const [ids, data] of docs) {
    
                    // @ts-ignore
                    const grouping = Map.groupBy([data], elem => elem[join.$groupby!])
    
                    for(const [group] of grouping) {
    
                        if(groupedDocs.has(group)) groupedDocs.get(group)!.set(ids, data)
                        else groupedDocs.set(group, new Map([[ids, data]]))
                    }
                }
    
                if(join.$onlyIds) {
    
                    const groupedIds: Map<T[keyof T] | U[keyof U], _ulid[]> = new Map<T[keyof T] | U[keyof U], _ulid[]>()
    
                    for(const [group, doc] of groupedDocs) {
                        groupedIds.set(group, Array.from(doc.keys()).flat())
                    }
    
                    return groupedIds
                }
                
                return groupedDocs
            }
    
            if(join.$onlyIds) return Array.from(new Set(Array.from(docs.keys()).flat()))    

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.joinDocs -> ${e.message}`)
        }

        return docs
    }

    static findDocs<T extends Record<string, any>>(collection: string, query?: _storeQuery<T>) {

        return {

            async *[Symbol.asyncIterator]() {
                
                for await (const data of Dir.searchDocs<T>(Query.getExprs(query ?? {}, collection), { updated: query?.$updated, created: query?.$created }, true)) {
                    
                    const doc = data as Map<_ulid, T>
                    
                    if(query && query.$onlyIds && doc.size > 0) {

                        await Bun.sleep(100)

                        for(let [_id] of doc) yield _id

                        continue
                    }
                    else if(doc.size > 0) {

                        for(let [_id, data] of doc) {

                            if(query && query.$select && query.$select.length > 0) {

                                data = Stawrij.selectValues<T>(query.$select as Array<keyof T>, data)
                            }

                            if(query && query.$rename) data = Stawrij.renameFields<T>(query.$rename, data)

                            await Bun.sleep(100)

                            yield new Map([[_id, data]]) as Map<_ulid, T>

                            continue
                        }
                    }
                }
            },
            
            async *collect() {

                let count = 0

                for await (const data of Dir.searchDocs<T>(Query.getExprs(query ?? {}, collection), { updated: query?.$updated, created: query?.$created })) {
                    
                    const doc = data as Map<_ulid, T>

                    if(doc.size > 0) {

                        count++

                        for(let [_id, data] of doc) {

                            if(query && query.$select && query.$select.length > 0) {

                                data = Stawrij.selectValues<T>(query.$select as Array<keyof T>, data)
                            }
    
                            if(query && query.$rename) data = Stawrij.renameFields<T>(query.$rename, data)
    
                            doc.set(_id, data)
                        }

                        if(query && query.$groupby) {

                            const docGroup: Map<T[keyof T] | undefined, Map<_ulid, Partial<T>>> = new Map<T[keyof T] | undefined, Map<_ulid, T>>()

                            for(const [id, data] of doc) {

                                const grouping = Map.groupBy([data], elem => elem[query.$groupby! as keyof T])

                                for(const [group] of grouping) {

                                    if(docGroup.has(group)) docGroup.get(group)!.set(id, data)
                                    else docGroup.set(group, new Map([[id, data]]))
                                }
                            }

                            if(query && query.$onlyIds) {

                                const groupedIds: Map<T[keyof T] | undefined, _ulid[]> = new Map<T[keyof T] | undefined, _ulid[]>()

                                for(const [group, doc] of docGroup) {
                                    groupedIds.set(group, Array.from(doc.keys()))
                                }

                                yield groupedIds
                                if(query && query.$limit && count === query.$limit) break
                                continue
                            }

                            yield docGroup
                            if(query && query.$limit && count === query.$limit) break
                            continue
                        }

                        if(query && query.$onlyIds) {
                            yield Array.from(doc.keys())[0]
                            if(query && query.$limit && count === query.$limit) break
                            continue
                        }

                        yield doc
                        if(query && query.$limit && count === query.$limit) break
                    }
                }
            },

            async *onDelete() {

                for await (const _id of Dir.searchDocs<T>(Query.getExprs(query ?? {}, collection), {}, true, true)) {
                    yield _id as _ulid
                }
            }
        }
    }
}