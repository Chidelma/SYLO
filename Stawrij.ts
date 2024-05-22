import Blob from "./Azure/Blob";
import Store from "./GCP/Storage";
import S3 from "./AWS/S3";
import Cluster from "./FDB/Cluster" 
import { S3Client } from "@aws-sdk/client-s3";
import { BlobServiceClient } from "@azure/storage-blob";
import { Storage } from '@google-cloud/storage'
import { _storeQuery } from "./types/query";
import { unlinkSync } from "node:fs";
import { Glob as gb } from "glob";
import { Glob } from "bun";
import Query from './Kweeree'
import { _schema } from './types/schema'
import { Database, open } from "foundationdb";

export default class Stawrij {

    private s3?: S3Client
    private blob?: BlobServiceClient
    private stawr?: Storage
    private db?: Database

    private static readonly PLATFORM = process.env.PLATFORM

    private static readonly INDEX_PATH = process.env.INDEX_PREFIX ?? `${process.cwd()}/db`

    private static readonly AWS = 'AWS'
    private static readonly AZURE = 'AZURE'
    private static readonly GCP = 'GCP'

    private static readonly ID_KEY = "_id"

    constructor({ S3Client, blobClient, storageClient }: { S3Client?: S3Client, blobClient?: BlobServiceClient, storageClient?: Storage }) {

        if(S3Client) this.s3 = S3Client
        if(blobClient) this.blob = blobClient
        if(storageClient) this.stawr = storageClient

        if(!S3Client && !blobClient && !storageClient) this.db = open()
    }

    private async getColData(silo: string, prefix: string) {

        const promises: Promise<string[]>[] = []

        if(this.s3) promises.push(S3.getColData(this.s3, silo, prefix))
            
        if(this.blob) promises.push(Blob.getColData(this.blob, silo, prefix))
        
        if(this.stawr) promises.push(Store.getColData(this.stawr, silo, prefix))

        if(this.db) promises.push(Cluster.getColData(this.db, prefix))

        return await Promise.race(promises)
    }

    async getDoc<T extends _schema<T>>(silo: string, collection: string, id: string, listen?: (id: string) => void) {

        let doc: T = {} as T

        try {

            const promises: Promise<T>[] = []

            const queue = new Set<string>()

            if(listen) {

                new gb(`${collection}:*:${id}`, {
                    cwd: Stawrij.INDEX_PATH,
                    ignore: {
                        ignored: p => queue.has(p.name)
                    }
                }).stream().on("data", path => {
                    const id = path.split(':').pop()!
                    queue.add(id)
                    setTimeout(() => {
                        listen(id)
                        queue.clear()
                    }, 2000);
                })
            }

            switch(Stawrij.PLATFORM) {
                case Stawrij.AWS:
                    promises.push(S3.getDoc(this.s3!, silo, collection, id, Stawrij.constructDoc))
                    break
                case Stawrij.AZURE:
                    promises.push(Blob.getDoc(this.blob!, silo, collection, id, Stawrij.constructDoc))
                    break
                case Stawrij.GCP:
                    promises.push(Store.getDoc(this.stawr!, silo, collection, id, Stawrij.constructDoc))
                    break
                default:
                    promises.push(Cluster.getDoc(this.db!, collection, id, Stawrij.constructDoc))
                    break
            }

            doc = await Promise.race(promises)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.getDoc -> ${e.message}`)
        }

        return doc
    }

    private async putData(silo: string, key: string) {

        const promises: Promise<void>[] = []

        if(this.s3) promises.push(S3.putData(this.s3, silo, key))
            
        if(this.blob) promises.push(Blob.putData(this.blob, silo, key))
        
        if(this.stawr) promises.push(Store.putData(this.stawr, silo, key))

        if(this.db) promises.push(Cluster.putData(this.db, key))

        await Promise.all(promises)
    }

    async putDoc<T extends _schema<T>>(silo: string, collection: string, doc: T) {
        
        try {

            doc._id = doc._id ?? crypto.randomUUID()

            const indexes = Stawrij.createIndexes(doc).map((idx) => `${Stawrij.INDEX_PATH}/${collection}:${idx}`)

            this.updateIndexes(silo, collection, doc._id, new Set(indexes))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.putDoc -> ${e.message}`)
        }

    }

    async putDocSQL<T extends _schema<T>>(silo: string, sql: string, collection?: string) {

        try {

            const insertSchema = Query.convertInsert<T>(sql)

            const doc: T = {} as T

            const keys = Object.keys(insertSchema).filter(key => !key.startsWith('$'))

            for(const key of keys) {
                doc[key as keyof Omit<T, '_id'>] = (insertSchema as T)[key as keyof Omit<T, '_id'>]
            }

            doc._id = crypto.randomUUID()

            await this.putDoc(silo, collection ?? insertSchema.$collection!, doc)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrji.putDocsSQL-> ${e.message}`)
        }
    }

    async patchDoc<T extends _schema<T>>(silo: string, collection: string, doc: Partial<T>) {
        
        try {

            if(!doc._id) throw new Error("This document does not contain an _id")

            const keys = Stawrij.deconstructDoc(collection, doc._id! as string, doc)

            await this.updateIndexes(silo, collection, doc._id! as string, new Set(keys))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.putDoc -> ${e.message}`)
        }
    }


    async patchDocsSQL<T extends _schema<T>>(silo: string, sql: string, collection?: string) {

        let count = 0

        try {

            const updateSchema = Query.convertUpdate<T>(sql)

            const coll = collection ?? updateSchema.$collection!

            const expressions = await Query.getExprs(updateSchema.$where!, coll)

            const indexes = await Promise.all(expressions.map((expr) => Array.fromAsync(new Glob(expr).scan({ cwd: Stawrij.INDEX_PATH }))))

            const keys = Object.keys(updateSchema).filter(key => !key.startsWith('$'))

            await Promise.all(indexes.flat().map(idx => idx.split(':').pop()!).map(id => {

                const partialDoc: Record<string, any> = { _id: id }

                for(const key of keys) partialDoc[key] = updateSchema[key as keyof Omit<T, '_id'>]
                
                return this.patchDoc(silo, coll, partialDoc)
            }))

            count = indexes.flat().length

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.patchDocSQL -> ${e.message}`)
        }

        return count
    }

    private async updateIndexes(silo: string, collection: string, id: string, newIndexes: Set<string>) {

        try {

            const extractValue = (file: string) => {
                const segements = file.split(':')
                return segements[segements.length - 2]
            }
    
            const indexes = await Array.fromAsync(new Glob(`${collection}:*:${id}`).scan({ cwd: Stawrij.INDEX_PATH }))
    
            const oldIndexes = new Set(indexes)
    
            const oldValues = new Set(Array.from(oldIndexes).map(extractValue))
            const newValues = new Set(Array.from(newIndexes).map(extractValue))
    
            const valuesToRemove = new Set(Array.from(oldValues).filter((val) => !newValues.has(val)))
            const valuesToAdd = new Set(Array.from(newValues).filter((val) => !oldValues.has(val)))
    
            const toRemove = new Set(Array.from(oldIndexes).filter((dir) => valuesToRemove.has(extractValue(dir))))
            const toAdd = new Set(Array.from(newIndexes).filter((dir) => valuesToAdd.has(extractValue(dir))))

            await Promise.all(Array.from(toRemove).map(index => {
                unlinkSync(index)
                const segments = index.split(':')
                const collIdx = segments.findIndex(seg => seg === collection)
                return this.delData(silo, segments.slice(collIdx).join(':'))
            }))

            await Promise.all(Array.from(toAdd).map(async index => {
                await Bun.write(index, '.')
                const segments = index.split(':')
                const collIdx = segments.findIndex(seg => seg === collection)
                return this.putData(silo, segments.slice(collIdx).join(':'))
            }))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.updateIndexes -> ${e.message}`)
        }
    }

    private async delData(silo: string, key: string) {

        const promises: Promise<void>[] = []

        if(this.s3) promises.push(S3.delData(this.s3, silo, key))
            
        if(this.blob) promises.push(Blob.delData(this.blob, silo, key))
        
        if(this.stawr) promises.push(Store.delData(this.stawr, silo, key))

        if(this.db) promises.push(Cluster.delData(this.db, key))

        await Promise.all(promises)
    }

    async delDoc(silo: string, collection: string, id: string) {

        try {

            const promises: Promise<void>[] = []

            if(this.s3) promises.push(S3.delDoc(this.s3, silo, collection, id))

            if(this.blob) promises.push(Blob.delDoc(this.blob, silo, collection, id))

            if(this.stawr) promises.push(Store.delDoc(this.stawr, silo, collection, id))

            if(this.db) promises.push(Cluster.delDoc(this.db, collection, id))

            await Promise.all(promises)

            const indexes = await Array.fromAsync(new Glob(`${collection}:*:${id}`).scan({ cwd: Stawrij.INDEX_PATH }))

            for(const idx of indexes) unlinkSync(idx)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.delDoc -> ${e.message}`)
        }
    }

    async delDocsSQL<T extends _schema<T>>(silo: string, sql: string, collection?: string) {

        let count = 0

        try {

            const deleteSchema = Query.convertDelete<T>(sql)

            const coll = collection ?? deleteSchema.$collection!

            const expressions = await Query.getExprs(deleteSchema, coll)

            const indexes = await Promise.all(expressions.map((expr) => Array.fromAsync(new Glob(expr).scan({ cwd: Stawrij.INDEX_PATH }))))

            await Promise.all(indexes.flat().map(idx => idx.split(':').pop()!).map(id => this.delDoc(silo, coll, id)))

            count = indexes.flat().length

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.delDocSQL -> ${e.message}`)
        }

        return count
    }

    private static deconstructDoc<T extends _schema<T>>(collection: string, id: string, obj: Record<string, any>, parentKey?: string) {

        const keys: string[] = []

        delete obj[this.ID_KEY]

        for(const key in obj) {

            const newKey = parentKey ? `${parentKey}:${key}` : key

            if(typeof obj[key] === 'object' && !Array.isArray(obj[key])) {
                keys.push(...this.deconstructDoc(collection, id, obj[key], newKey))
            } else if(typeof obj[key] === 'object' && Array.isArray(obj[key])) {
                const items: (string | number | boolean)[] = obj[key]
                if(items.some((item) => typeof item === 'object')) throw new Error(`Cannot have an array of objects`)
                items.map((item, idx) => keys.push(`${collection}:${id}:${newKey}:${idx}:${item}`))
            } else {
                keys.push(`${collection}:${id}:${newKey}`)
            }
        }

        return keys
    }

    private static constructDoc(keys: string[]) {

        const doc: Record<string, any> = {}

        for(const key of keys) {

            const segements = key.split(':')

            doc._id = segements[1]

            const path = segements.slice(2, -1)
            const value = segements[segements.length - 1]
            const index = !isNaN(Number(value)) ? parseInt(value, 10) : NaN

            let currObj = doc

            for(let i = 0; i < path.length; i++) {

                const segment = path[i]

                const isLastSegment = i === path.length - 1

                if(isLastSegment) {
                    if(!isNaN(index)) {
                        if(!Array.isArray(currObj[segment])) currObj[segment] = []
                        currObj[segment][index] = this.parseValue(value)
                    } else currObj[segment] = this.parseValue(value)
                } else {
                    if(!currObj[segment]) currObj[segment] = {}
                    currObj = currObj[segment]
                }
            }
        }

        const convertToArray = (obj: any) => {

            if(typeof obj === "object" && obj !== null) {
                for(const key in obj) {
                    if(obj.hasOwnProperty(key)) {
                        const value = obj[key]
                        if(typeof value == 'object' && value !== null) obj[key] = convertToArray(value)
                    }
                }

                const numericKeys = Object.keys(obj).every(k => !isNaN(Number(k)))
                if(numericKeys) return Object.keys(obj).sort((a, b) => Number(a) - Number(b)).map(k => obj[k])
            }

            return obj
        }

        return convertToArray(doc)
    }

    private static createIndexes(doc: Record<string, any>, parentKey?: string) {

        const indexes: string[] = []

        const id = doc[this.ID_KEY]

        delete doc[this.ID_KEY]

        for(const key in doc) {

            const newKey = parentKey ? `${parentKey}:${key}` : key

            if(typeof doc[key] === 'object' && !Array.isArray(doc[key])) {
                indexes.push(...this.createIndexes(doc[key], newKey))
            } else if(typeof doc[key] === 'object' && Array.isArray(doc[key])) {
                const items: (string | number | boolean)[] = doc[key]
                if(items.some((item) => typeof item === 'object')) throw new Error(`Cannot have an array of objects`)
                items.map((item, idx) => indexes.push(`${newKey}:${item}:${idx}:${id}`))
            } else {
                indexes.push(`${newKey}:${id}`)
            }
        }

        return indexes
    }

    async findDocsSQL<T extends _schema<T>>(silo: string, collection: string, sql: string, listen?: (ids: string[]) => void) {

        let results: T[] = []

        try {

            results = await this.findDocs(silo, collection, Query.convertQuery<T>(sql), listen)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.findDocsSQL-> ${e.message}`)
        }

        return results
    }

    async findDocs<T extends _schema<T>>(silo: string, collection: string, query: _storeQuery<T>, listen?: (ids: string[]) => void) {

        let results: T[] = []

        try {

            const queue = new Set<string>()

            const expressions = await Query.getExprs(query, collection)

            if(listen) {

                new gb(expressions, {
                    cwd: Stawrij.INDEX_PATH,
                    ignore: {
                        ignored: p => queue.has(p.name)
                    }
                }).stream().on("data", path => {
                    queue.add(path.split(':').pop()!)
                    setTimeout(() => {
                        listen(Array.from(queue))
                        queue.clear()
                    }, 2000);
                })
            }

            const indexes = await Promise.all(expressions.map((expr) => Array.fromAsync(new Glob(expr).scan({ cwd: Stawrij.INDEX_PATH }))))

            results = await this.execOpIndexes(silo, collection, indexes.flat(), query)
            
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

    private async execOpIndexes<T extends _schema<T>>(silo: string, collection: string, indexes: string[], query: _storeQuery<T>) {

        let results: T[] = []

        try {

            let ids = Array.from(new Set(indexes.map((idx) => idx.split(':').pop()!)))

            if(query.$limit) ids = ids.slice(0, query.$limit)

            if(query.$select && query.$select.length > 0) {

                for(const id of ids) {

                    const prefixes = query.$select.map(column => `${collection}:${id}:${String(column)}`)

                    const colData = await Promise.all(prefixes.map(prefix => this.getColData(silo, prefix)))

                    results.push(Stawrij.constructDoc(colData.flat()))
                }

            } else results = await Promise.all(ids.map((id) => this.getDoc<T>(silo, collection, id)))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.execOpIndexes -> ${e.message}`)
        }

        return results
    }

    private static parseValue(value: string) {
    
        const num = Number(value) 

        if(!Number.isNaN(num)) return num

        if(value === "true") return true

        if(value === "false") return false

        if(value === 'null') return null
    
        return value
    }
}