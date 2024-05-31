import { S3Client } from "@aws-sdk/client-s3";
import { BlobServiceClient } from "@azure/storage-blob";
import { Storage } from '@google-cloud/storage'
import { _storeDelete, _storeQuery, _storeUpdate } from "./types/query";
import Query from './Kweeree'
import { _schema, _keyval } from './types/schema'
import { RedisClientType, RedisFunctions, RedisModules, RedisScripts } from "@redis/client";
import Dir from "./Index/Directory";

export default class Stawrij {

    static s3?: S3Client
    static blob?: BlobServiceClient
    static stawr?: Storage
    static redis?: RedisClientType<RedisModules, RedisFunctions, RedisScripts>
    private static useFS = false

    private static indexUrl = new URL('./Index/worker.ts', import.meta.url).href
    private static s3Url = new URL('./AWS/worker.ts', import.meta.url).href
    private static azUrl = new URL('./Azure/worker.ts', import.meta.url).href
    private static gcpUrl = new URL('./GCP/worker.ts', import.meta.url).href
    private static redUrl = new URL('./Redis/worker.ts', import.meta.url).href
    private static fsUrl = new URL('./FS/worker.ts', import.meta.url).href

    static configureStorages({ S3Client, blobClient, storageClient, redisClient }: { S3Client?: S3Client, blobClient?: BlobServiceClient, storageClient?: Storage, redisClient?: RedisClientType }) {

        if(S3Client) Stawrij.s3 = S3Client
        if(blobClient) Stawrij.blob = blobClient
        if(storageClient) Stawrij.stawr = storageClient
        if(redisClient) Stawrij.redis = redisClient

        if(!S3Client && !blobClient && !storageClient && !redisClient) Stawrij.useFS = true
    }

    static invokeWorker(url: string, message: any, resolve: () => void, result?: any) {

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

    static async getDoc<T extends _schema<T>>(collection: string, id: string, onChange?: (doc: T) => void, onDelete?: () => void) {

        let doc: T = {} as T

        try {

            const recontructDoc = async () => {

                const fileIndexes = await Dir.searchIndexes(`${collection}/**/${id}`, true)
                const dirIndexes = await Dir.searchIndexes(`${collection}/**/${id}/`)
                
                const keyVals = await Dir.reArrangeIndexes([...fileIndexes, ...dirIndexes])

                let keyVal: Record<string, string> = {}

                keyVals.map(keyval => keyval.data).forEach(data => {
                    const segs = data.split('/')
                    const val = segs.pop()!
                    const key = segs.join('/')
                    keyVal = { ...keyVal, [key]: val }
                })
                
                return Dir.constructDoc(keyVal, id) as T
            }

            if(onChange) Dir.onAdd(`${collection}/**/${id}`, async () => onChange(await recontructDoc()))

            if(onDelete) Dir.onDelete(`${collection}/**/${id}`, onDelete)

            doc = await recontructDoc()

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.getDoc -> ${e.message}`)
        }

        return doc
    }

    static async putDoc<T extends _schema<T>>(silo: string, collection: string, doc: T) {
        
        try {

            doc._id = doc._id ?? crypto.randomUUID()

            console.log(`Writing ${doc._id}`)

            const keyVals = Dir.deconstructDoc(collection, doc._id, doc)

            await Promise.all(keyVals.map(keyval => {

                const promises: Promise<void>[] = [new Promise<void>(resolve => this.invokeWorker(this.indexUrl, { action: 'PUT', idx: keyval.index }, resolve))]

                const segments = keyval.data.split('/')

                const val = segments.pop()!
                
                const key = segments.join('/')

                const message = { action: 'PUT', data: { silo, key, val } }
                
                if(Stawrij.s3) promises.push(new Promise<void>(resolve => this.invokeWorker(this.s3Url, message, resolve)))

                if(Stawrij.blob) promises.push(new Promise<void>(resolve => this.invokeWorker(this.azUrl, message, resolve)))

                if(Stawrij.stawr) promises.push(new Promise<void>(resolve => this.invokeWorker(this.gcpUrl, message, resolve)))

                if(Stawrij.redis) promises.push(new Promise<void>(resolve => this.invokeWorker(this.redUrl, message, resolve)))

                if(Stawrij.useFS) promises.push(new Promise<void>(resolve => this.invokeWorker(this.fsUrl, message, resolve)))
            
                return promises
            }).flat())

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.putDoc -> ${e.message}`)
        }

    }

    static async putDocSQL<T extends _schema<T>>(silo: string, sql: string, collection?: string) {

        try {

            const insertSchema = Query.convertInsert<T>(sql)

            const doc: T = {} as T

            const keys = Object.keys(insertSchema).filter(key => !key.startsWith('$'))

            for(const key of keys) {
                doc[key as keyof Omit<T, '_id'>] = (insertSchema as T)[key as keyof Omit<T, '_id'>]
            }

            doc._id = crypto.randomUUID()

            await Stawrij.putDoc(silo, collection ?? insertSchema.$collection!, doc)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrji.putDocsSQL-> ${e.message}`)
        }
    }

    static async patchDoc<T extends _schema<T>>(silo: string, collection: string, doc: Partial<T>) {
        
        try {

            if(!doc._id) throw new Error("Stawrij document does not contain an _id")

                console.log(`Updating ${doc._id}`)

            await this.putDoc(silo, collection, doc as T)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.patchDoc -> ${e.message}`)
        }
    }


    static async patchDocs<T extends _schema<T>>(silo: string, collection: string, updateSchema: _storeUpdate<T>) {

        let count = 0
        
        try {

            const expressions = await Query.getExprs(updateSchema.$where!, collection)

            const indexes = await Dir.searchIndexes(expressions)

            const keys = Object.keys(updateSchema).filter(key => !key.startsWith('$'))

            const ids = Array.from(new Set(indexes.map(idx => idx.split('/').pop()!)))

            await Promise.all(ids.map(id => {

                const partialDoc: Record<string, any> = { _id: id }

                for(const key of keys) partialDoc[key] = updateSchema[key as keyof Omit<T, '_id'>]
                
                return Stawrij.patchDoc(silo, collection, partialDoc)
            }))

            count = ids.length

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.putDoc -> ${e.message}`)
        }

        return count
    }


    static async patchDocsSQL<T extends _schema<T>>(silo: string, sql: string, collection?: string) {

        let count = 0

        try {

            const updateSchema = Query.convertUpdate<T>(sql)

            count = await Stawrij.patchDocs(silo, collection ?? updateSchema.$collection!, updateSchema)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.patchDocSQL -> ${e.message}`)
        }

        return count
    }

    static async delDoc(silo: string, collection: string, id: string) {

        try {

            console.log(`Deleting ${id}`)

            const fileIndexes = await Dir.searchIndexes(`${collection}/**/${id}`, true)
            const dirIndexes = await Dir.searchIndexes(`${collection}/**/${id}/`)

            const keyVals = await Dir.reArrangeIndexes([...fileIndexes, ...dirIndexes])

            await Promise.all(keyVals.map(keyval => {

                const promises: Promise<void>[] = [new Promise<void>(resolve => this.invokeWorker(this.indexUrl, { action: 'DEL', idx: keyval.index }, resolve))]

                const segments = keyval.data.split('/')

                segments.pop()!

                const message = { action: 'DEL', data: { silo, key: segments.join('/') } }
                
                if(Stawrij.s3) promises.push(new Promise<void>(resolve => this.invokeWorker(this.s3Url, message, resolve)))

                if(Stawrij.blob) promises.push(new Promise<void>(resolve => this.invokeWorker(this.azUrl, message, resolve)))

                if(Stawrij.stawr) promises.push(new Promise<void>(resolve => this.invokeWorker(this.gcpUrl, message, resolve)))

                if(Stawrij.redis) promises.push(new Promise<void>(resolve => this.invokeWorker(this.redUrl, message, resolve)))

                if(Stawrij.useFS) promises.push(new Promise<void>(resolve => this.invokeWorker(this.fsUrl, message, resolve)))
            
                return promises
            }).flat())

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.delDoc -> ${e.message}`)
        }
    }

    static async delDocs<T extends _schema<T>>(silo: string, collection: string, deleteSchema: _storeDelete<T>) {

        let count = 0

        try {

            const expressions = await Query.getExprs(deleteSchema, collection)

            const indexes = [...await Dir.searchIndexes(expressions, true), ...await Dir.searchIndexes(expressions)]

            const keyVals = await Dir.reArrangeIndexes(indexes)

            await Promise.all(keyVals.map(keyval => {

                const promises: Promise<void>[] = [new Promise<void>(resolve => this.invokeWorker(this.indexUrl, { action: 'DEL', idx: keyval.index }, resolve))]

                const segments = keyval.data.split('/')

                segments.pop()!

                const message = { action: 'DEL', data: { silo, key: segments.join('/') } }
                
                if(Stawrij.s3) promises.push(new Promise<void>(resolve => this.invokeWorker(this.s3Url, message, resolve)))

                if(Stawrij.blob) promises.push(new Promise<void>(resolve => this.invokeWorker(this.azUrl, message, resolve)))

                if(Stawrij.stawr) promises.push(new Promise<void>(resolve => this.invokeWorker(this.gcpUrl, message, resolve)))

                if(Stawrij.redis) promises.push(new Promise<void>(resolve => this.invokeWorker(this.redUrl, message, resolve)))

                if(Stawrij.useFS) promises.push(new Promise<void>(resolve => this.invokeWorker(this.fsUrl, message, resolve)))
            
                return promises

            }).flat())

            count = indexes.length

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.delDocs -> ${e.message}`)
        }

        return count
    }

    static async delDocsSQL<T extends _schema<T>>(silo: string, sql: string, collection?: string) {

        let count = 0

        try {

            const deleteSchema = Query.convertDelete<T>(sql)

            count = await Stawrij.delDocs(silo, collection ?? deleteSchema.$collection!, deleteSchema)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.delDocsSQL -> ${e.message}`)
        }

        return count
    }
    

    static async findDocsSQL<T extends _schema<T>>(sql: string, collection?: string, onAdd?: (doc: T) => void, onDelete?: (id: string) => void) {

        let results: T[] = []

        try {

            const query = Query.convertQuery<T>(sql)

            results = await Stawrij.findDocs(collection ?? query.$collection!, query, onAdd, onDelete)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.findDocsSQL-> ${e.message}`)
        }

        return results
    }

    static async findDocs<T extends _schema<T>>(collection: string, query: _storeQuery<T>, onAdd?: (doc: T) => void, onDelete?: (id: string) => void) {

        let results: T[] = []

        try {

            const expressions = await Query.getExprs(query, collection)

            if(onAdd) Dir.onAdd(expressions, async (id: string) => {
                onAdd(await this.getDoc(collection, id))
            })

            if(onDelete) Dir.onDelete(expressions, onDelete)

            const indexes = [...await Dir.searchIndexes(expressions, true), ...await Dir.searchIndexes(expressions)]
            
            const groupByUUID = (indexes: string[]) => {

                const groupMap = new Map<string, string[]>()

                indexes.forEach(idx => {
                    const id = idx.split('/').pop()!
                    if(!groupMap.has(id)) groupMap.set(id, [idx])
                    else groupMap.get(id)?.push(idx)
                })

                return groupMap
            }

            let res: Record<string, any>[] = []

            for(const [id, currIndexes] of groupByUUID(indexes).entries()) {
                const keyVals = await Dir.reArrangeIndexes(currIndexes)
                let keyVal: Record<string, string> = {}
                keyVals.map(keyval => keyval.data).forEach(data => {
                    const segs = data.split('/')
                    const val = segs.pop()!
                    const key = segs.join('/')
                    keyVal = { ...keyVal, [key]: val }
                })
                res.push(Dir.constructDoc(keyVal, id) as T)
            }

            if(query.$limit) res = res.slice(0, query.$limit)

            if(query.$select && query.$select.length > 0) {

                for(const result of res) {
                    for(const column in result) {
                        if(!query.$select.includes(column as keyof T)) delete result[column]
                    }
                }
            }

            results = res as T[]
            
            if(query.$sort) {
                for(const col in query.$sort) {
                    if(query.$sort[col as keyof Omit<T, '_id'>] === "asc") results.sort((a, b) => {

                        const aVal = a[col as keyof Omit<T, '_id'>]
                        const bVal = b[col as keyof Omit<T, '_id'>]

                        if(typeof aVal === "string" && typeof bVal === "string") return (aVal as string).localeCompare(bVal)

                        if(aVal < bVal) return -1
                        if(aVal > bVal) return 1

                        return 0
                    })
                    else results.sort((a, b) => {

                        const aVal = a[col as keyof Omit<T, '_id'>]
                        const bVal = b[col as keyof Omit<T, '_id'>]

                        if(typeof aVal === "string" && typeof bVal === "string") return (bVal as string).localeCompare(aVal)

                        if(aVal < bVal) return 1
                        if(aVal > bVal) return -1

                        return 0
                    })
                }
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.findDocs -> ${e.message}`)
        }

        return results
    }
}