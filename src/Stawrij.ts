import Query from './Kweeree'
import Paser from './Paza'
import Dir from "./Directory";
import { invokeWorker } from "./utils/general";

export default class Stawrij {

    private static indexUrl = new URL('./workers/Directory.ts', import.meta.url).href
    private static storeUrl = new URL('./workers/Stawrij.ts', import.meta.url).href

    private static SCHEMA = (process.env.SCHEMA || 'STRICT') as _schema

    static async executeSQL<T>(SQL: string) {

        const op = SQL.match(/^(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|TRUNCATE|DROP|USE)/i)

        if(!op) throw new Error("Missing SQL Operation")

        switch(op[0]) {
            case "USE":
                return Paser.convertUse(SQL)
            case "CREATE":
                return await Stawrij.createSchema(Paser.convertTableCRUD(SQL).collection!)
            case "ALTER":   
                return await Stawrij.modifySchema(Paser.convertTableCRUD(SQL).collection!)
            case "TRUNCATE":
                return await Stawrij.truncateSchema(Paser.convertTableCRUD(SQL).collection!)
            case "DROP":
                return Stawrij.dropSchema(Paser.convertTableCRUD(SQL).collection!)
            case "SELECT":
                const query = Paser.convertSelect<T>(SQL)
                const selCol = query.$collection
                delete query.$collection
                return Stawrij.findDocs(selCol!, query) as _storeCursor<T>
            case "INSERT":
                const insert = Paser.convertInsert<T>(SQL)
                const insCol = insert.$collection
                delete insert.$collection
                return await Stawrij.putData(insCol!, insert)
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

    static async truncateSchema(collection: string) {
        try {
            await Dir.truncateSchema(collection)
        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.truncateSchema -> ${e.message}`)
        }
    }

    static dropSchema(collection: string) {
        try {
            Dir.dropSchema(collection)
        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.dropSchema -> ${e.message}`)
        }
    }

    static getDoc<T>(collection: string, _id: _uuid, onlyId: boolean = false) {

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
                return new Map([[_id, await Dir.reconstructData<T>(collection, _id)]]) as Map<_uuid, T>
            },

            async *onDelete() {
                for await (const _ of Dir.onDelete(`${collection}/**/${_id}`)) yield _id
            }
        }
    }

    static async bulkDataPut<T>(collection: string, data: T[]) {

        await Promise.all(data.map(doc => new Promise<void>(resolve => invokeWorker(Stawrij.storeUrl, { action: "PUT", data: { collection, doc }}, resolve))))
    }

    static async putData<T extends object>(collection: string, data: Map<_uuid, T> | T) {

        const _id = data instanceof Map ? Array.from((data as Map<_uuid, T>).keys())[0] : crypto.randomUUID() 
        
        try {

            console.log(`Writing ${_id}`)

            if(this.SCHEMA === 'STRICT') await Dir.validateData(collection, data)

            await Dir.aquireLock(collection, _id)
            
            const doc = data instanceof Map ? (data as Map<_uuid, T>).get(_id)! : data as T

            const indexes = Dir.deconstructData(collection, _id, doc)

            await Promise.all(indexes.map(idx => new Promise<void>(resolve => invokeWorker(this.indexUrl, { action: 'PUT', data: { idx } }, resolve))))

            await Dir.releaseLock(collection, _id)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.putDoc -> ${e.message}`)
        }

        return _id
    }

    static async patchDoc<T>(collection: string, doc: Map<_uuid, Partial<T>>) {
        
        try {

            const _id = Array.from(doc.keys())[0] as _uuid

            if(!_id) throw new Error("Stawrij document does not contain an UUID")

            console.log(`Updating ${_id}`)

            const currData = await Dir.reconstructData<T>(collection, _id)

            const data = doc.get(_id)!

            // @ts-ignore
            for(const field in data) currData[field] = data[field]!

            await this.putData(collection, new Map([[_id, currData]]) as Map<_uuid, T>)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.patchDoc -> ${e.message}`)
        }
    }


    static async patchDocs<T>(collection: string, updateSchema: _storeUpdate<T>) {

        let count = 0
        
        try {

            const indexes = await Dir.searchIndexes(Query.getExprs(updateSchema.$where!, collection))

            const fields = Object.keys(updateSchema).filter(key => !key.startsWith('$'))

            const ids = Array.from(new Set(indexes.map(idx => idx.split('/').pop()!)))

            await Promise.all(ids.map(_id => {

                const partialData: Record<string, any> = { }

                for(const field of fields) partialData[field] = updateSchema[field as keyof T]
                
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

            console.log(`Deleting ${_id}`)

            const indexes = await Dir.searchIndexes(`${collection}/**/${_id}`)

            await Promise.all(indexes.map(idx => new Promise<void>(resolve => invokeWorker(this.indexUrl, { action: 'DEL', data: { idx } }, resolve))))

            await Dir.releaseLock(collection, _id)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.delDoc -> ${e.message}`)
        }
    }

    static async delDocs<T>(collection: string, deleteSchema?: _storeDelete<T>) {

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

    static findDocs<T>(collection: string, query?: _storeQuery<T>) {

        return {

            async *[Symbol.asyncIterator]() {

                const initRes = await this.collect()

                if((initRes instanceof Map && initRes.size > 0) || (Array.isArray(initRes) && initRes.length > 0)) {
                    for(const doc of initRes) yield doc
                }
                
                for await (const _id of Dir.onChange(Query.getExprs(query ?? {}, collection))) {

                    if(query && query.$onlyIds) yield _id
                    else {
                        
                        const data = await Dir.reconstructData<T>(collection, _id)

                        if(query && query.$select && query.$select.length > 0) {

                            for(const field in data) {
                                if(!query.$select.includes(field as keyof T)) delete data[field]
                            }

                            yield new Map([[_id, data]]) as Map<_uuid, Partial<T>>
                        
                        } else yield new Map([[_id, data]]) as Map<_uuid, T>
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

                    const data = await Dir.reconstructData<T>(collection, _id)

                    if(query && query.$select && query.$select.length > 0) {

                        for(const field in data) {
                            if(!query.$select.includes(field as keyof T)) delete data[field]
                        }

                        docs.set(_id, data)

                    } else docs.set(_id, data)
                }

                if(query && query.$groupby) {

                    const groupedDocs: Map<keyof T, Map<_uuid, Partial<T>>> = new Map<keyof T, Map<_uuid, T>>()

                    for(const [id, data] of docs.entries()) {

                        const grouping = Map.groupBy([data], _ => query.$groupby!)

                        for(const [group] of grouping.entries()) {

                            if(groupedDocs.has(group)) groupedDocs.get(group)!.set(id, data)
                            else groupedDocs.set(group, new Map([[id, data]]))
                        }
                    }

                    if(query && query.$onlyIds) {

                        const groupedIds: Map<keyof T, _uuid[]> = new Map<keyof T, _uuid[]>()

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