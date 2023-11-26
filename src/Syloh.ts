import { Blob } from "./Azure/Blob";
import { Store } from "./GCP/Storage";
import { S3 } from "./AWS/S3";
import { S3Client } from "@aws-sdk/client-s3";
import { BlobServiceClient } from "@azure/storage-blob";
import { Storage } from '@google-cloud/storage'

export class Syloh {

    private s3: S3Client
    private blob: BlobServiceClient
    private store: Storage

    private static readonly DELIMITER = '\t\b\n'

    constructor({ S3Client, blobClient, storageClient }: { S3Client?: S3Client, blobClient?: BlobServiceClient, storageClient?: Storage }) {

        if(S3Client) this.s3 = S3Client
        if(blobClient) this.blob = blobClient
        if(storageClient) this.store = storageClient
    }

    async getDoc<T extends object>(silo: string, collection: string, id: string, idKey: string, hitCache?: (prefix: string) => Promise<T>) {

        let doc: T = {} as T

        try {

            if(hitCache) doc = await hitCache(`${collection}/${id}`)

            const promises: Promise<Record<string, any>>[] = []

            if(Object.keys(doc).length === 0 || hitCache === undefined) {

                if(this.s3) promises.push(S3.getDoc(this.s3, silo, collection, id))

                if(this.blob) promises.push(Blob.getDoc(this.blob, silo, collection, id))

                if(this.store) promises.push(Store.getDoc(this.store, silo, collection, id))

                const record = await Promise.race(promises)

                doc = Syloh.wrangleRecord<T>(record, idKey)
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(e.message)
        }

        return doc as T
    }

    async putDoc<T>(silo: string, collection: string, doc: T, idKey: string, checkCache?: (key: string) => Promise<boolean>) {

        try {

            const promises: Promise<void>[] = []

            const record = Syloh.unwrangleDoc<T>(doc, idKey)

            const paths = new Map<string, any[]>()

            for(const [key, value] of record) {

                const path = `${collection}/${doc[idKey]}/${key}`
                const search = `${collection}/${key}/${doc[idKey]}`

                if(checkCache) {
                    const modified = await checkCache(path)
                    if(modified) paths.set(path, [search, value])
                } else paths.set(path, [search, value])
            }

            if(this.s3) {
                for(const [path, [search, value]] of paths) {
                    promises.push(S3.putData(this.s3, silo, path, value))
                    promises.push(S3.putData(this.s3, silo, search, ''))
                }
            }

            if(this.blob) {
                for(const [path, [search, value]] of paths) {
                    promises.push(Blob.putData(this.blob, silo, path, value))
                    promises.push(Blob.putData(this.blob, silo, search, ''))
                }
            }

            if(this.store) {
                for(const [path, [search, value]] of paths) {
                    promises.push(Store.putData(this.store, silo, path, value))
                    promises.push(Store.putData(this.store, silo, search, ''))
                }
            }

            await Promise.all(promises)

        } catch(e) {
            if(e instanceof Error) throw new Error(e.message)
        }
    }

    async delDoc(silo: string, collection: string, id: string, delCache?: (prefix: string) => Promise<void>) {

        try {

            const promises: Promise<void>[] = []

            if(this.s3) promises.push(S3.delDoc(this.s3, silo, collection, id))

            if(this.blob) promises.push(Blob.delDoc(this.blob, silo, collection, id))

            if(this.store) promises.push(Store.delDoc(this.store, silo, collection, id))

            if(delCache) promises.push(delCache(`${collection}/${id}`))

            await Promise.all(promises)

        } catch(e) {
            if(e instanceof Error) throw new Error(e.message)
        }
    }

    private static unwrangleDoc<T>(doc: T, idKey: string, parentKey?: string) {

        const result = new Map<string, any>()

        for (const key in doc) {

            if(key !== idKey) {

                const newKey = parentKey ? `${parentKey}/${key}` : key

                if (typeof doc[key] === 'object' && !Array.isArray(doc[key]) && doc[key] !== null) {
                    Object.assign(result, this.unwrangleDoc(doc[key], newKey))
                } else if(typeof doc[key] === 'object' && Array.isArray(doc[key])) {
                    if(Array.from(doc[key] as any[]).some((idx) => typeof idx === 'object')) throw new Error('Cannot have an array of objects')
                    result.set(newKey, Array.from(doc[key] as any[]).join(Syloh.DELIMITER))
                } else {
                    result.set(newKey, doc[key])
                }
            }
        }

        return result
    }

    private static wrangleRecord<T>(record: Record<string, string>, idKey: string) {

        const result: Record<string, any> = {}
    
        try {

            for(const key in record) {

                const allAttrs = key.split('/')

                const attrs = allAttrs.slice(2)
    
                let currentObj = result
        
                for (let i = 0; i < attrs.length; i++) {
    
                    const attr = attrs[i]
        
                    if(i === attrs.length - 1) currentObj[attr] = Syloh.parseValue(record[key])
                    else {
                        currentObj[attr] = currentObj[attr] || {}
                        currentObj = currentObj[attr]
                    }
                }

                result[idKey] = allAttrs[1]
            }
    
        } catch (e) {
            if (e instanceof Error) throw new Error(`S3.wrangleObject -> ${e.message}`)
        }
    
        return result as T
    }

    private static parseValue(value: string) {

        try {

            if(value.includes(this.DELIMITER)) return value.split(this.DELIMITER)

            const num = Number(value) 

            if(!Number.isNaN(num)) return num

            if(value === 'true') return true

            if(value === 'false') return false

            if(value !== 'null') return value

        } catch (e) {
            if (e instanceof Error) throw new Error(`S3.parseValue -> ${e.message}`)
        }

        return null
    }
}