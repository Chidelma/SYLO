import { _storeDelete, _storeQuery, _storeUpdate } from "./types/query";
import Query from './Kweeree'
import { _schema } from './types/schema'
import { _keyval } from "./types/general";
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

    static getDoc<T extends _schema<T>>(collection: string, id: string, onlyId: boolean = false) {

        return {

            async *[Symbol.asyncIterator]() {

                for await (const _ of Dir.onChange(`${collection}/**/${id}/*`, "addDir", true)) {
                    if(onlyId) yield id
                    else {
                        yield await Dir.reconstructDoc<T>(collection, id)
                    }
                }
            },

            async once() {

                let doc: T = {} as T

                await Dir.onChange(`${collection}/**/${id}/*`, "addDir").next({ count: 1, limit: 1 })

                doc = await Dir.reconstructDoc<T>(collection, id)
                
                return doc
            },

            async *onDelete() {
                for await (const _ of Dir.onChange(`${collection}/**/${id}/*`, "unlinkDir", true)) yield id
            }
        }
    }

    static async bulkPutDocs<T extends _schema<T>>(collection: string, docs: T[]) {

        await Promise.all(docs.map(doc => new Promise<void>(resolve => Stawrij.invokeWorker(Stawrij.storeUrl, { action: "PUT", data: { collection, doc }}, resolve))))
    }

    static async putDoc<T extends _schema<T>>(collection: string, doc: T) {

        const _id = doc._id ?? crypto.randomUUID()
        
        try {

            await Dir.aquireLock(collection, _id)

            doc._id = _id

            console.log(`Writing ${doc._id}`)

            const indexes = Dir.deconstructDoc(collection, doc._id, doc).map(idx => `${idx}/${Object.keys(doc).length}`)

            await Promise.all(indexes.map(idx => new Promise<void>(resolve => this.invokeWorker(this.indexUrl, { action: 'PUT', data: { idx } }, resolve))))

            await Dir.releaseLock(collection, _id)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.putDoc -> ${e.message}`)
        }

        return _id
    }

    static async putDocSQL<T extends _schema<T>>(sql: string, collection?: string) {

        let _id: any

        try {

            const insertSchema = Query.convertInsert<T>(sql)

            const doc: T = {} as T

            const keys = Object.keys(insertSchema).filter(key => !key.startsWith('$'))

            for(const key of keys) {
                doc[key as keyof Omit<T, '_id'>] = (insertSchema as T)[key as keyof Omit<T, '_id'>]
            }

            _id = await Stawrij.putDoc(collection ?? insertSchema.$collection!, doc)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrji.putDocsSQL-> ${e.message}`)
        }

        return _id
    }

    static async patchDoc<T extends _schema<T>>(collection: string, doc: Partial<T>) {
        
        try {

            if(!doc._id) throw new Error("Stawrij document does not contain an _id")

            console.log(`Updating ${doc._id}`)

            const currDoc = await Dir.reconstructDoc<T>(collection, doc._id as string)

            for(const key in doc) currDoc[key] = doc[key]!

            await this.putDoc(collection, currDoc)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.patchDoc -> ${e.message}`)
        }
    }


    static async patchDocs<T extends _schema<T>>(collection: string, updateSchema: _storeUpdate<T>) {

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

                const partialDoc: Record<string, any> = { _id: id }

                for(const key of keys) partialDoc[key] = updateSchema[key as keyof Omit<T, '_id'>]
                
                return new Promise<void>(resolve => Stawrij.invokeWorker(Stawrij.storeUrl, { action: 'PATCH', data: { collection, doc: partialDoc } }, resolve))
            }))

            count = ids.length

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.putDoc -> ${e.message}`)
        }

        return count
    }


    static async patchDocsSQL<T extends _schema<T>>(sql: string, collection?: string) {

        let count = 0

        try {

            const updateSchema = Query.convertUpdate<T>(sql)

            count = await Stawrij.patchDocs(collection ?? updateSchema.$collection!, updateSchema)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.patchDocSQL -> ${e.message}`)
        }

        return count
    }

    static async delDoc(collection: string, id: string) {

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

    static async delDocs<T extends _schema<T>>(collection: string, deleteSchema: _storeDelete<T>) {

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

    static async delDocsSQL<T extends _schema<T>>(sql: string, collection?: string) {

        let count = 0

        try {

            const deleteSchema = Query.convertDelete<T>(sql)

            count = await Stawrij.delDocs(collection ?? deleteSchema.$collection!, deleteSchema)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.delDocsSQL -> ${e.message}`)
        }

        return count
    }
    

    static findDocsSQL<T extends _schema<T>>(sql: string, onlyIds: boolean = false, collection?: string) {

        const query = Query.convertQuery<T>(sql)

        return Stawrij.findDocs(collection ?? query.$collection!, query, onlyIds)
    }

    static findDocs<T extends _schema<T>>(collection: string, query: _storeQuery<T>, onlyIds: boolean = false) {

        return {

            async *[Symbol.asyncIterator]() {

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

                            yield result as T
                        
                        } else yield doc
                    }
                }
            },
            
            async next(limit?: number) {
                
                const results: T[] = []

                const ids: string[] = []

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

                            results.push(subRes)
                        
                        } else results.push(doc)
                    }

                    if(limit) count++
                }

                return onlyIds ? ids : results
            },

            async *onDelete() {

                const expressions = await Query.getExprs(query, collection)

                for await (const id of Dir.onChange(expressions, "unlinkDir", true)) yield id
            }
        }
    }
}