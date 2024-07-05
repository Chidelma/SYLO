import { _storeDelete, _storeQuery, _storeUpdate } from "./types/query";
import Query from './Kweeree'
import Paser from './Paza'
import { _fullMerge, _storeCursor, _uuid } from './types/schema'
import Dir from "./Index/Directory";

export default class Stawrij {

    private static indexUrl = new URL('./workers/Directory.ts', import.meta.url).href
    private static storeUrl = new URL('./workers/Stawrij.ts', import.meta.url).href

    private static invokeWorker(url: string, message: any, resolve: () => void, result?: any) {

        const worker = new Worker(url)

        worker.onmessage = ev => {
            if(result) {
                if(Array.isArray(result)) result.push(ev.data)
                else result = ev.data
            } 
            worker.terminate()
            resolve()
        }

        worker.onerror = ev => {
            console.error(ev.message)
            worker.terminate()
            resolve()
        }
        
        worker.postMessage(message)
    }

    static async executeSQL<T>(SQL: string, params?: Map<keyof T, any>) {

        const op = SQL.match(/^(SELECT|INSERT|UPDATE|DELETE)/i)

        if(!op) throw new Error("MIssing SQL Operation")

        switch(op[0]) {

            case "SELECT":
                const query = Paser.convertSelect<T>(SQL)
                const selCol = query.$collection
                delete query.$collection
                return Stawrij.findDocs(selCol!, query) as _storeCursor<T>
            case "INSERT":
                const insert = Paser.convertInsert<T>(SQL, params)
                const insCol = insert.$collection
                delete insert.$collection
                return await Stawrij.putDoc(insCol!, insert)
            case "UPDATE":
                const update = Paser.convertUpdate<T>(SQL, params)
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

    static getDoc<T>(collection: string, id: _uuid, onlyId: boolean = false) {

        return {

            async *[Symbol.asyncIterator]() {

                for await (const _ of Dir.onChange(`${collection}/**/${id}/*`, "addDir", true)) {
                    if(onlyId) yield id
                    else {
                        const doc = await Dir.reconstructDoc(collection, id)
                        yield new Map([[id, doc]]) as Map<_uuid, T>
                    }
                }
            },

            async once() {

                await Dir.onChange(`${collection}/**/${id}/*`, "addDir").next({ count: 1, limit: 1 })

                const doc = await Dir.reconstructDoc(collection, id)

                return new Map([[id, doc]]) as Map<_uuid, T>
            },

            async *onDelete() {
                for await (const _ of Dir.onChange(`${collection}/**/${id}/*`, "unlinkDir", true)) yield id
            }
        }
    }

    static async bulkPutDocs<T>(collection: string, docs: T[]) {

        await Promise.all(docs.map(doc => new Promise<void>(resolve => Stawrij.invokeWorker(Stawrij.storeUrl, { action: "PUT", data: { collection, doc }}, resolve))))
    }

    static async putDoc<T extends object>(collection: string, data: _fullMerge<T>) {

        const _id = data instanceof Map ? Array.from((data as Map<_uuid, T>).keys())[0] : crypto.randomUUID() 
        
        try {

            console.log(`Writing ${_id}`)

            await Dir.aquireLock(collection, _id)
            
            const doc = data instanceof Map ? (data as Map<_uuid, T>).get(_id)! : data as T

            const indexes = Dir.deconstructDoc(collection, _id, doc).map(idx => `${idx}/${Object.keys(doc).length}`)

            await Promise.all(indexes.map(idx => new Promise<void>(resolve => this.invokeWorker(this.indexUrl, { action: 'PUT', data: { idx } }, resolve))))

            await Dir.releaseLock(collection, _id)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.putDoc -> ${e.message}`)
        }

        return _id
    }

    static async patchDoc<T>(collection: string, data: Map<_uuid, Partial<T>>) {
        
        try {

            const _id = Array.from(data.keys())[0] as _uuid

            if(!_id) throw new Error("Stawrij document does not contain an UUID")

            console.log(`Updating ${_id}`)

            const currDoc = await Dir.reconstructDoc<T>(collection, _id)

            const doc = data.get(_id)!

            // @ts-ignore
            for(const key in doc) currDoc[key] = doc[key]!

            await this.putDoc(collection, new Map([[_id, currDoc]]) as Map<_uuid, T>)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.patchDoc -> ${e.message}`)
        }
    }


    static async patchDocs<T>(collection: string, updateSchema: _storeUpdate<T>) {

        let count = 0
        
        try {

            const expressions = await Query.getExprs(updateSchema.$where!, collection)

            const indexes = await Dir.searchIndexes(expressions)

            const keys = Object.keys(updateSchema).filter(key => !key.startsWith('$'))

            const ids = Array.from(new Set(indexes.filter(Dir.hasUUID).map(idx => {
                const segs = idx.split('/')
                segs.pop()!
                return segs.pop()!
            })))

            await Promise.all(ids.map(id => {

                const partialDoc: Record<string, any> = { }

                for(const key of keys) partialDoc[key] = updateSchema[key as keyof T]
                
                return new Promise<void>(resolve => Stawrij.invokeWorker(Stawrij.storeUrl, { action: 'PATCH', data: { collection, doc: new Map([[id, partialDoc]]) } }, resolve))
            }))

            count = ids.length

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.putDoc -> ${e.message}`)
        }

        return count
    }

    static async delDoc(collection: string, id: _uuid) {

        try {

            await Dir.aquireLock(collection, id)

            console.log(`Deleting ${id}`)

            const indexes = [...await Dir.searchIndexes(`${collection}/**/${id}/*`, true), ...await Dir.searchIndexes(`${collection}/**/${id}/*/`)]

            await Promise.all(indexes.map(idx => new Promise<void>(resolve => this.invokeWorker(this.indexUrl, { action: 'DEL', data: { idx } }, resolve))))

            await Dir.releaseLock(collection, id)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.delDoc -> ${e.message}`)
        }
    }

    static async delDocs<T>(collection: string, deleteSchema: _storeDelete<T>) {

        let count = 0

        try {

            const expressions = await Query.getExprs(deleteSchema, collection)

            const indexes = [...await Dir.searchIndexes(expressions, true), ...await Dir.searchIndexes(expressions)]

            const ids = Array.from(new Set(indexes.filter(Dir.hasUUID).map(idx => {
                const segs = idx.split('/')
                segs.pop()!
                return segs.pop()!
            })))

            await Promise.all(ids.map(id => new Promise<void>(resolve => Stawrij.invokeWorker(Stawrij.storeUrl, { action: 'DEL', data: { collection, id }}, resolve))))
            
            count = ids.length

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.delDocs -> ${e.message}`)
        }

        return count
    }

    static findDocs<T>(collection: string, query: _storeQuery<T>, onlyIds: boolean = false) {

        return {

            async *[Symbol.asyncIterator](): AsyncGenerator<Map<_uuid, T> | Map<_uuid, Partial<T>> | _uuid, void, unknown> {

                const expressions = await Query.getExprs(query, collection)

                for await (const id of Dir.onChange(expressions, "addDir", true)) {

                    if(onlyIds) yield id
                    else {
                        
                        const doc = await Dir.reconstructDoc<T>(collection, id)

                        if(query.$select && query.$select.length > 0) {
                            
                            const result = {...doc}

                            for(const col in result) {
                                if(!query.$select.includes(col as keyof T)) delete result[col]
                            }

                            yield new Map([[id, result]]) as Map<_uuid, Partial<T>>
                        
                        } else yield new Map([[id, doc]]) as Map<_uuid, T>
                    }
                }
            },
            
            async next(limit?: number): Promise<Map<_uuid, T> | Map<_uuid, Partial<T>> | _uuid[]> {
                
                const results: Map<_uuid, T> | Map<_uuid, Partial<T>> = new Map<_uuid, T>()

                const ids: _uuid[] = []

                const expressions = await Query.getExprs(query, collection)

                const iter = Dir.onChange(expressions, "addDir")

                let count = 0
                
                while(true) {

                    const res = await iter.next({ count, limit })
                    
                    if(res.done || count === limit) break

                    if(onlyIds) ids.push(res.value)
                    else {

                        const doc = await Dir.reconstructDoc<T>(collection, res.value)

                        if(query.$select && query.$select.length > 0) {

                            const subRes = {...doc}

                            for(const col in subRes) {
                                if(!query.$select.includes(col as keyof T)) delete subRes[col]
                            }

                            results.set(res.value, subRes)

                        } else results.set(res.value, doc)
                    }

                    if(limit) count++
                }

                return onlyIds ? ids : results
            },

            async *onDelete(): AsyncGenerator<_uuid, void, unknown> {

                const expressions = await Query.getExprs(query, collection)

                for await (const id of Dir.onChange(expressions, "unlinkDir", true)) yield id
            }
        }
    }
}