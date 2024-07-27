import Query from './Kweeree'
import Paser from './Paza'
import Dir from "./Directory";
import { invokeWorker } from "./utils/general";

export default class Stawrij {

    private static indexUrl = new URL('./workers/Directory.ts', import.meta.url).href
    private static storeUrl = new URL('./workers/Stawrij.ts', import.meta.url).href

    private static SCHEMA = (process.env.SCHEMA || 'STRICT') as _schema

    private static LOGGING = process.env.LOGGING === 'true'

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
                return Stawrij.findDocs(selCol! as string, query as _storeQuery<T>) as _storeCursor<T>
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

    static dropSchema(collection: string) {
        try {
            Dir.dropSchema(collection)
        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.dropSchema -> ${e.message}`)
        }
    }

    static getDoc<T extends Record<string, any>>(collection: string, _id: _uuid, onlyId: boolean = false) {

        return {

            async *[Symbol.asyncIterator]() {

                const initRes = await this.once()

                if(initRes.size > 0) yield initRes

                for await (const _ of Dir.onChange(`${collection}/**/${_id}`)) {
                    if(onlyId) yield _id
                    else {
                        const doc = await Dir.reconstructData(collection, _id)
                        yield new Map([[_id, doc]]) as Map<_uuid, T>
                    }
                }
            },

            async once() {

                const data = await Dir.reconstructData<T>(collection, _id)

                const doc = new Map<_uuid, T>()

                if(Object.entries(data).length > 0) doc.set(_id, data)

                return doc
            },

            async *onDelete() {
                for await (const _ of Dir.onDelete(`${collection}/**/${_id}`)) yield _id
            }
        }
    }

    static async bulkDataPut<T extends Record<string, any>>(collection: string, data: T[]) {

        const ids: _uuid[] = []

        await Promise.all(data.map(doc => new Promise<void>(resolve => invokeWorker(Stawrij.storeUrl, { action: "PUT", data: { collection, doc }}, resolve, ids))))
    
        return ids.flat()
    }

    static async putData<T extends Record<string, any>>(collection: string, data: Map<_uuid, T> | T) {

        const _id = data instanceof Map ? Array.from((data as Map<_uuid, T>).keys())[0] : crypto.randomUUID() 
        
        try {   

            if(this.LOGGING) console.log(`Writing ${_id}`)

            if(this.SCHEMA === 'STRICT') await Dir.validateData(collection, data)

            await Dir.aquireLock(collection, _id)
            
            const doc = data instanceof Map ? (data as Map<_uuid, T>).get(_id)! : data as T

            const indexes = Dir.deconstructData(collection, _id, doc)

            await Promise.all(indexes.map(idx => new Promise<void>(resolve => invokeWorker(this.indexUrl, { action: 'PUT', data: { idx } }, resolve))))

            await Dir.releaseLock(collection, _id)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.putData -> ${e.message}`)
        }

        return _id
    }

    static async patchDoc<T extends Record<string, any>>(collection: string, doc: Map<_uuid, Partial<T>>) {
        
        try {

            const _id = Array.from(doc.keys())[0] as _uuid

            if(!_id) throw new Error("Stawrij document does not contain an UUID")

            if(this.LOGGING) console.log(`Updating ${_id}`)

            const currData = await Dir.reconstructData<T>(collection, _id)

            const data = doc.get(_id)!

            // @ts-ignore
            for(const field in data) currData[field] = data[field]!

            await this.putData(collection, new Map([[_id, currData]]) as Map<_uuid, T>)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.patchDoc -> ${e.message}`)
        }
    }


    static async patchDocs<T extends Record<string, any>>(collection: string, updateSchema: _storeUpdate<T>) {

        let count = 0
        
        try {

            const indexes = await Dir.searchIndexes(Query.getExprs(updateSchema.$where ?? {}, collection))

            const ids = Array.from(new Set(indexes.map(idx => idx.split('/').pop()!)))

            await Promise.all(ids.map(_id => {

                const partialData: Record<keyof Partial<T>, any> = { } as Record<keyof Partial<T>, any>

                for(const field in updateSchema.$set) partialData[field as keyof T] = updateSchema.$set[field as keyof T]
                
                return new Promise<void>(resolve => invokeWorker(Stawrij.storeUrl, { action: 'PATCH', data: { collection, doc: new Map([[_id, partialData]]) } }, resolve))
            }))

            count = ids.length

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.putDoc -> ${e.message}`)
        }

        return count
    }

    static async delDoc(collection: string, _id: _uuid) {

        try {

            await Dir.aquireLock(collection, _id)

            if(this.LOGGING) console.log(`Deleting ${_id}`)

            const indexes = await Dir.searchIndexes(`${collection}/**/${_id}`)

            await Promise.all(indexes.map(idx => new Promise<void>(resolve => invokeWorker(this.indexUrl, { action: 'DEL', data: { idx } }, resolve))))

            await Dir.releaseLock(collection, _id)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.delDoc -> ${e.message}`)
        }
    }

    static async delDocs<T extends Record<string, any>>(collection: string, deleteSchema?: _storeDelete<T>) {

        let count = 0

        try {

            const indexes = await Dir.searchIndexes(Query.getExprs(deleteSchema ?? {}, collection))

            const ids = Array.from(new Set(indexes.map(idx => idx.split('/').pop()!)))

            await Promise.all(ids.map(_id => new Promise<void>(resolve => invokeWorker(Stawrij.storeUrl, { action: 'DEL', data: { collection, _id }}, resolve))))
            
            count = ids.length

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
        
        const docs: Map<_uuid[], T | U | T & U | Partial<T> & Partial<U>> = new Map<_uuid[], T | U | T & U | Partial<T> & Partial<U>>()

        try {

            const compareFields = async (leftField: keyof T, rightField: keyof U, compare: (leftVal: string, rightVal: string) => boolean) => {

                try {

                    if(join.$leftCollection === join.$rightCollection) throw new Error("Left and right collections cannot be the same")

                    const [leftFieldIndexes, rightFieldIndexes] = await Promise.all([Dir.searchIndexes(`${join.$leftCollection}/${String(leftField)}/**`), Dir.searchIndexes(`${join.$rightCollection}/${String(rightField)}/**`)])
                
                    for(const leftIdx of leftFieldIndexes) {
        
                        const leftSegs = leftIdx.split('/')
                        const left_id = leftSegs.pop()! as _uuid
                        const leftVal = leftSegs.pop()!
        
                        const leftCollection = leftSegs.shift()!
        
                        const allVals = new Set<string>()
        
                        for(const rightIdx of rightFieldIndexes) {
        
                            const rightSegs = rightIdx.split('/')
                            const right_id = rightSegs.pop()! as _uuid
                            const rightVal = rightSegs.pop()!
        
                            const rightCollection = rightSegs.shift()!
        
                            if(compare(rightVal, leftVal) && !allVals.has(rightVal)) {
        
                                allVals.add(rightVal)
        
                                switch(join.$mode) {
                                    case "inner":
                                        docs.set([left_id, right_id], { [leftField]: Dir.parseValue(leftVal), [rightField]: Dir.parseValue(rightVal) } as Partial<T> & Partial<U>)
                                        break
                                    case "left":
                                        let leftData = await Dir.reconstructData<T>(leftCollection, left_id)
                                        if(join.$select) leftData = this.selectValues<T>(join.$select as Array<keyof T>, leftData)
                                        if(join.$rename) leftData = this.renameFields<T>(join.$rename, leftData)
                                        docs.set([left_id, right_id], leftData as T)
                                        break
                                    case "right":
                                        let rightData = await Dir.reconstructData<U>(rightCollection, right_id)
                                        if(join.$select) rightData = this.selectValues<U>(join.$select as Array<keyof U>, rightData)
                                        if(join.$rename) rightData = this.renameFields<U>(join.$rename, rightData)
                                        docs.set([left_id, right_id], rightData as U)
                                        break
                                    case "outer":
                                        let [leftFullData, rightFullData] = await Promise.all([Dir.reconstructData<T>(leftCollection, left_id), Dir.reconstructData<U>(rightCollection, right_id)])
                                        if(join.$select) {
                                            leftFullData = this.selectValues<T>(join.$select as Array<keyof T>, leftFullData) as Awaited<T>
                                            rightFullData = this.selectValues<U>(join.$select as Array<keyof U>, rightFullData) as Awaited<U>
                                        }
                                        if(join.$rename) {
                                            leftFullData = this.renameFields<T>(join.$rename, leftFullData) as Awaited<T>
                                            rightFullData = this.renameFields<U>(join.$rename, rightFullData) as Awaited<U>
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
    
                const groupedDocs: Map<T[keyof T] | U[keyof U], Map<_uuid[], Partial<T | U>>> = new Map<T[keyof T] | U[keyof U], Map<_uuid[], Partial<T | U>>>()
    
                for(const [ids, data] of docs.entries()) {
    
                    // @ts-ignore
                    const grouping = Map.groupBy([data], elem => elem[join.$groupby!])
    
                    for(const [group] of grouping.entries()) {
    
                        if(groupedDocs.has(group)) groupedDocs.get(group)!.set(ids, data)
                        else groupedDocs.set(group, new Map([[ids, data]]))
                    }
                }
    
                if(join.$onlyIds) {
    
                    const groupedIds: Map<T[keyof T] | U[keyof U], _uuid[]> = new Map<T[keyof T] | U[keyof U], _uuid[]>()
    
                    for(const [group, doc] of groupedDocs.entries()) {
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

                const data = await this.collect()

                if((Array.isArray(data) && data.length > 0) || (data instanceof Map && data.size > 0)) yield data
                
                for await (const _id of Dir.onChange(Query.getExprs(query ?? {}, collection))) {

                    if(query && query.$onlyIds) yield _id
                    else {
                        
                        let data = await Dir.reconstructData<T>(collection, _id)

                        if(query && query.$select && query.$select.length > 0) {

                            data = Stawrij.selectValues<T>(query.$select as Array<keyof T>, data)
                        }

                        if(query && query.$rename) data = Stawrij.renameFields<T>(query.$rename, data)

                        yield new Map([[_id, data]]) as Map<_uuid, T>
                    }
                }
            },
            
            async collect() {

                const docs: Map<_uuid, T> | Map<_uuid, Partial<T>> = new Map<_uuid, T>()

                const ids = new Set<_uuid>()

                const indexes = await Dir.searchIndexes(Query.getExprs(query ?? {}, collection))

                for(let i = 0; i < indexes.length; i++) {
                    ids.add(indexes[i].split('/').pop()! as _uuid)
                    if(query && query.$limit && ids.size === query.$limit) break
                }

                for(const _id of ids) {

                    let data = await Dir.reconstructData<T>(collection, _id)

                    if(query && query.$select && query.$select.length > 0) {

                        data = Stawrij.selectValues<T>(query.$select as Array<keyof T>, data)
                    } 

                    if(query && query.$rename) data = Stawrij.renameFields<T>(query.$rename, data)

                    docs.set(_id, data)
                }

                if(query && query.$groupby) {

                    const groupedDocs: Map<T[keyof T] | undefined, Map<_uuid, Partial<T>>> = new Map<T[keyof T] | undefined, Map<_uuid, T>>()

                    for(const [id, data] of docs.entries()) {

                        const grouping = Map.groupBy([data], elem => elem[query.$groupby! as keyof T])

                        for(const [group] of grouping.entries()) {

                            if(groupedDocs.has(group)) groupedDocs.get(group)!.set(id, data)
                            else groupedDocs.set(group, new Map([[id, data]]))
                        }
                    }

                    if(query && query.$onlyIds) {

                        const groupedIds: Map<T[keyof T] | undefined, _uuid[]> = new Map<T[keyof T] | undefined, _uuid[]>()

                        for(const [group, doc] of groupedDocs.entries()) {
                            groupedIds.set(group, Array.from(doc.keys()))
                        }

                        return groupedIds
                    }

                    return groupedDocs
                }

                if(query && query.$onlyIds) return Array.from(ids)

                return docs
            },

            async *onDelete() {

                for await (const _id of Dir.onDelete(Query.getExprs(query ?? {}, collection))) {
                    yield _id
                }
            }
        }
    }
}