import Blob from "./Azure/Blob";
import Store from "./GCP/Storage";
import S3 from "./AWS/S3";
import { S3Client } from "@aws-sdk/client-s3";
import { BlobServiceClient } from "@azure/storage-blob";
import { Storage } from '@google-cloud/storage'
import { _storeQuery } from "./types/query";
import { Glob } from 'bun'
import { mkdirSync, rmdirSync } from "node:fs";
import { watch } from 'chokidar'
import Query from './Kweeree'

export default class Stawrij {

    private s3?: S3Client
    private blob?: BlobServiceClient
    private stawr?: Storage

    private static readonly PLATFORM = process.env['PLATFORM']!

    private static readonly INDEX_PATH = process.env.INDEX_PREFIX!

    private static readonly AWS = 'AWS'
    private static readonly AZURE = 'AZURE'
    private static readonly GCP = 'GCP'

    constructor({ S3Client, blobClient, storageClient }: { S3Client?: S3Client, blobClient?: BlobServiceClient, storageClient?: Storage }) {

        if(S3Client) this.s3 = S3Client
        if(blobClient) this.blob = blobClient
        if(storageClient) this.stawr = storageClient
    }

    async getDoc<T extends Record<string, any>>(silo: string, collection: string, id: string, listen?: (doc: T) => void) {

        let doc: T = {} as T

        try {

            const promises: Promise<T>[] = []

            const queue = new Set<string>()

            if(Stawrij.PLATFORM) {

                switch(Stawrij.PLATFORM) {
                    case Stawrij.AWS:
                        promises.push(S3.getDoc(this.s3!, silo, collection, id))
                        break
                    case Stawrij.AZURE:
                        promises.push(Blob.getDoc(this.blob!, silo, collection, id))
                        break
                    case Stawrij.GCP:
                        promises.push(Store.getDoc(this.stawr!, silo, collection, id))
                        break
                    default:
                        if(this.s3) promises.push(S3.getDoc(this.s3, silo, collection, id))
                        if(this.blob) promises.push(Blob.getDoc(this.blob, silo, collection, id))
                        if(this.stawr) promises.push(Store.getDoc(this.stawr, silo, collection, id))
                        break
                }

            } else {

                if(this.s3) promises.push(S3.getDoc(this.s3, silo, collection, id))
                if(this.blob) promises.push(Blob.getDoc(this.blob, silo, collection, id))
                if(this.stawr) promises.push(Store.getDoc(this.stawr, silo, collection, id))
            }

            doc = await Promise.race(promises)

            if(listen) {

                setInterval(async () => {
                    const id = Array.from(queue).shift()
                    if(id) listen(await this.getDoc(silo, collection, id))
                }, 2500)
                
                watch(`${collection}/**/*${String(id)}`, { cwd: Stawrij.INDEX_PATH })
                        .on("addDir", async (path) => queue.add(path.split('/').pop()!))
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.getDoc -> ${e.message}`)
        }

        return doc
    }

    async putDoc<T extends Record<string, any>>(silo: string, collection: string, id: string, doc: T) {
        
        try {

            const promises: Promise<void>[] = []

            const indexes = Stawrij.createIndexes(id, doc).map((idx) => `${Stawrij.INDEX_PATH}/${collection}/${idx}`)

            if(this.s3) promises.push(S3.putDoc(this.s3, silo, collection, id, doc))
            
            if(this.blob) promises.push(Blob.putDoc(this.blob, silo, collection, id, doc))
            
            if(this.stawr) promises.push(Store.putDoc(this.stawr, silo, collection, id, doc))

            await Promise.all(promises)

            await this.updateIndexes(collection, id, new Set(indexes))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.putDoc -> ${e.message}`)
        }

    }

    async patchDoc<T extends Record<string, any>>(silo: string, collection: string, id: string, doc: Partial<T>) {
        
        try {

            const fullDoc = await this.getDoc<T>(silo, collection, id)

            for(const key in doc) {
                if(fullDoc[key]) fullDoc[key] = doc[key]!
            }

            await this.putDoc<T>(silo, collection, id, fullDoc)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.putDoc -> ${e.message}`)
        }
    }

    private async updateIndexes(collection: string, id: string | number | symbol, newIndexes: Set<string>) {

        try {

            const extractValue = (directory: string) => {
                const paths = directory.split('/')
                const idx = paths.findIndex((dir) => dir === collection)
                return paths[idx + 2]
            }
    
            const indexes = await Array.fromAsync(new Glob(`${collection}/**/*${String(id)}`).scan({ cwd: Stawrij.INDEX_PATH }))
    
            const oldIndexes = new Set(indexes)
    
            const oldValues = new Set(Array.from(oldIndexes).map(extractValue))
            const newValues = new Set(Array.from(newIndexes).map(extractValue))
    
            const valuesToRemove = new Set(Array.from(oldValues).filter((val) => !newValues.has(val)))
            const valuesToAdd = new Set(Array.from(newValues).filter((val) => !oldValues.has(val)))
    
            const toRemove = new Set(Array.from(oldIndexes).filter((dir) => valuesToRemove.has(extractValue(dir))))
            const toAdd = new Set(Array.from(newIndexes).filter((dir) => valuesToAdd.has(extractValue(dir))))
    
            for(const idx of toRemove) rmdirSync(idx, { recursive: true })
            for(const idx of toAdd) mkdirSync(idx, { recursive: true })

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.updateIndexes -> ${e.message}`)
        }
    }

    async delDoc(silo: string, collection: string, id: string | number | symbol) {

        try {

            const promises: Promise<void>[] = []

            if(this.s3) promises.push(S3.delDoc(this.s3, silo, collection, id))

            if(this.blob) promises.push(Blob.delDoc(this.blob, silo, collection, id))

            if(this.stawr) promises.push(Store.delDoc(this.stawr, silo, collection, id))

            await Promise.all(promises)

            const indexes = await Array.fromAsync(new Glob(`${collection}/**/*${String(id)}`).scan({ cwd: Stawrij.INDEX_PATH }))

            for(const idx of indexes) rmdirSync(idx, { recursive: true })

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.delDoc -> ${e.message}`)
        }
    }

    private static createIndexes<T extends Record<string, any>>(id: string | number | symbol, doc: T, parentKey?: string) {

        const indexes: string[] = []

        for(const key in doc) {

            const newKey = parentKey ? `${parentKey}/${key}` : key

            if(typeof doc[key] === 'object' && !Array.isArray(doc[key])) {
                indexes.push(...this.createIndexes(id, doc[key], newKey))
            } else if(typeof doc[key] === 'object' && Array.isArray(doc[key])) {
                const items: (string | number | boolean)[] = doc[key]
                if(items.some((item) => typeof item === 'object')) throw new Error(`Cannot have an array of objects`)
                items.map((item) => indexes.push(`${newKey}/${item}/${String(id)}`))
            } else {
                indexes.push(`${newKey}/${String(id)}`)
            }
        }

        return indexes
    }

    async findDocsSQL<T extends Record<string, any>, U extends keyof T>(silo: string, collection: string, sql: string, listen?: (docs: T[]) => void) {

        let results: T[] = []

        try {

            results = await this.findDocs(silo, collection, Query.convert<T, U>(sql), listen)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrji.findDocsSQL-> ${e.message}`)
        }

        return results
    }

    async findDocs<T extends Record<string, any>, U extends keyof T>(silo: string, collection: string, query: _storeQuery<T, U>, listen?: (docs: T[]) => void) {

        let results: T[] = []

        try {

            let changed = false

            const expressions = await Query.getExprs(collection, query)

            const indexes = await Promise.all(expressions.map((expr) => Array.fromAsync(new Glob(expr).scan({ cwd: Stawrij.INDEX_PATH }))))

            results = await this.execOpIndexes(silo, collection, indexes.flat())
            
            if(query.$limit) results = results.slice(0, query.$limit)
            if(query.$sort) {
                for(const col in query.$sort) {
                    if(query.$sort[col as keyof Omit<T, U>] === "asc") results.sort((a, b) => a[col as keyof Omit<T, U>].localCompare(b[col as keyof Omit<T, U>]))
                    else results.sort((a, b) => b[col as keyof Omit<T, U>].localCompare(a[col as keyof Omit<T, U>]))
                }
            }

            if(listen) {

                setInterval(async () => {
                    if(changed) {
                        listen(await this.findDocs(silo, collection, query))
                        changed = false
                    }
                }, 2500)

                watch(expressions, { cwd: Stawrij.INDEX_PATH })
                    .on("change", async () => changed = true)
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.findDocs -> ${e.message}`)
        }

        return results
    }

    private async execOpIndexes<T extends Record<string, any>>(silo: string, collection: string, indexes: string[]) {

        let results: T[] = []

        try {

            const ids = Array.from(new Set(indexes.map((idx) => idx.split('/').pop()!)))

            results = await Promise.all(ids.map((id) => this.getDoc<T>(silo, collection, id)))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Stawrij.execOpIndexes -> ${e.message}`)
        }

        return results
    }
}