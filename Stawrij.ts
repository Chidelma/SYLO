import Blob from "./Azure/Blob";
import Store from "./GCP/Storage";
import S3 from "./AWS/S3";
import { S3Client } from "@aws-sdk/client-s3";
import { BlobServiceClient } from "@azure/storage-blob";
import { Storage } from '@google-cloud/storage'

export default class Stawrij {

    private s3: S3Client
    private blob: BlobServiceClient
    private stawr: Storage

    private static readonly DELIMITER = '\t\b\n'

    private static readonly PLATFORM = process.env['PLATFORM']

    private static readonly AWS = 'AWS'
    private static readonly AZURE = 'AZURE'
    private static readonly GCP = 'GCP'

    constructor({ S3Client, blobClient, storageClient }: { S3Client?: S3Client, blobClient?: BlobServiceClient, storageClient?: Storage }) {

        if(S3Client) this.s3 = S3Client
        if(blobClient) this.blob = blobClient
        if(storageClient) this.stawr = storageClient
    }

    async getData(silo: string, path: string) {

        let data: any;

        try {

            const promises: Promise<string>[] = []

            if(Stawrij.PLATFORM) {

                switch(Stawrij.PLATFORM) {
                    case Stawrij.AWS:
                        promises.push(S3.getData(this.s3, silo, path))
                        break
                    case Stawrij.AZURE:
                        promises.push(Blob.getData(this.blob, silo, path))
                        break
                    case Stawrij.GCP:
                        promises.push(Store.getData(this.stawr, silo, path))
                        break
                    default:
                        if(this.s3) promises.push(S3.getData(this.s3, silo, path))
                        if(this.blob) promises.push(Blob.getData(this.blob, silo, path))
                        if(this.stawr) promises.push(Store.getData(this.stawr, silo, path))
                        break
                }
                
            } else {

                if(this.s3) promises.push(S3.getData(this.s3, silo, path))
                if(this.blob) promises.push(Blob.getData(this.blob, silo, path))
                if(this.stawr) promises.push(Store.getData(this.stawr, silo, path))
            }

            data = Stawrij.parseValue(await Promise.race(promises))

        } catch(e) {
            if(e instanceof Error) throw new Error(`this.getData -> ${e.message}`)
        }

        return data
    }

    async getDoc<T extends object>(silo: string, collection: string, id: string, idKey: string) {

        let doc: T | Record<string, any> = {}

        try {

            const promises: Promise<Record<string, any>>[] = []

            if(Stawrij.PLATFORM) {

                switch(Stawrij.PLATFORM) {
                    case Stawrij.AWS:
                        promises.push(S3.getDoc(this.s3, silo, collection, id))
                        break
                    case Stawrij.AZURE:
                        promises.push(Blob.getDoc(this.blob, silo, collection, id))
                        break
                    case Stawrij.GCP:
                        promises.push(Store.getDoc(this.stawr, silo, collection, id))
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

            doc = Stawrij.wrangleRecord<T>(await Promise.race(promises), idKey)

        } catch(e) {
            if(e instanceof Error) throw new Error(`this.getDoc -> ${e.message}`)
        }

        return doc
    }

    async putDoc<T extends object>(silo: string, collection: string, doc: T | Record<string, any>, idKey: string) {

        const searchIndexes: string[] = []
        
        try {

            const promises: Promise<void>[] = []

            const paths: Record<string, any> = {}

            const record = Stawrij.unwrangleDoc<T>(doc as T, idKey)

            for(const key in record) {

                const path = `${collection}/${doc[idKey]}/${key}`

                searchIndexes.push(`${collection}/${key}/${doc[idKey]}`)

                paths[path] = record[key]
            }

            for(const key in paths) {

                if(this.s3) {
                    promises.push(S3.putData(this.s3, silo, key, paths[key]))
                }

                if(this.blob) {
                    promises.push(Blob.putData(this.blob, silo, key, paths[key]))
                }

                if(this.stawr) {
                    promises.push(Store.putData(this.stawr, silo, key, paths[key]))
                }
            }

            await Promise.all(promises)

        } catch(e) {
            if(e instanceof Error) throw new Error(`this.putDoc -> ${e.message}`)
        }

        return searchIndexes
    }

    async delDoc(silo: string, collection: string, id: string) {

        try {

            const promises: Promise<void>[] = []

            if(this.s3) promises.push(S3.delDoc(this.s3, silo, collection, id))

            if(this.blob) promises.push(Blob.delDoc(this.blob, silo, collection, id))

            if(this.stawr) promises.push(Store.delDoc(this.stawr, silo, collection, id))

            await Promise.all(promises)

        } catch(e) {
            if(e instanceof Error) throw new Error(`this.delDoc -> ${e.message}`)
        }
    }

    async listKeys(silo: string, prefix: string, max?: number) {

        let keys: string[] = []

        try {

            const promises: Promise<string[]>[] = []

            if(Stawrij.PLATFORM) {

                switch(Stawrij.PLATFORM) {
                    case Stawrij.AWS:
                        promises.push(S3.listKeys(this.s3, silo, prefix, max))
                        break
                    case Stawrij.AZURE:
                        promises.push(Blob.listKeys(this.blob, silo, prefix))
                        break
                    case Stawrij.GCP:
                        promises.push(Store.listKeys(this.stawr, silo, prefix, max))
                        break
                    default:
                        if(this.s3) promises.push(S3.listKeys(this.s3, silo, prefix, max))
                        if(this.blob) promises.push(Blob.listKeys(this.blob, silo, prefix))
                        if(this.stawr) promises.push(Store.listKeys(this.stawr, silo, prefix, max))
                        break
                }

            } else {

                if(this.s3) promises.push(S3.listKeys(this.s3, silo, prefix, max))
                if(this.blob) promises.push(Blob.listKeys(this.blob, silo, prefix))
                if(this.stawr) promises.push(Store.listKeys(this.stawr, silo, prefix, max))
            }

            keys = await Promise.race(promises)

        } catch(e) {
            if(e instanceof Error) throw new Error(`this.listKeys -> ${e.message}`)
        }

        return keys
    }

    static unwrangleDoc<T>(doc: T, idKey: string, parentKey?: string) {

        const result: Record<string, any> = {}

        for (const key in doc) {

            if(key !== idKey) {

                const newKey = parentKey ? `${parentKey}/${key}` : key

                if (typeof doc[key] === 'object' && !Array.isArray(doc[key]) && doc[key] !== null) {
                    Object.assign(result, this.unwrangleDoc(doc[key], newKey))
                } else if(typeof doc[key] === 'object' && Array.isArray(doc[key])) {
                    if(Array.from(doc[key] as any[]).some((idx) => typeof idx === 'object')) throw new Error('Cannot have an array of objects')
                    result[newKey] = Array.from(doc[key] as any[]).join(this.DELIMITER)
                } else {
                    result[newKey] = doc[key]
                }
            }
        }

        return result
    }

    static wrangleRecord<T>(record: Record<string, any>, idKey: string) {

        const result: Record<string, any> = {}
    
        try {

            for(const key in record) {

                const allAttrs = key.split('/')

                const attrs = allAttrs.slice(2)
    
                let currentObj = result
        
                for (let i = 0; i < attrs.length; i++) {
    
                    const attr = attrs[i]
        
                    if(i === attrs.length - 1) currentObj[attr] = this.parseValue(record[key])
                    else {
                        currentObj[attr] = currentObj[attr] || {}
                        currentObj = currentObj[attr]
                    }
                }

                result[idKey] = allAttrs[1]
            }
    
        } catch (e) {
            if (e instanceof Error) throw new Error(`this.wrangleObject -> ${e.message}`)
        }
    
        return result as T
    }

    static parseValue(value: string) {

        try {

            if(value.includes(this.DELIMITER)) return value.split(this.DELIMITER)

            const num = Number(value) 

            if(!Number.isNaN(num)) return num

            if(value === 'true') return true

            if(value === 'false') return false

            if(value !== 'null') return value

        } catch (e) {
            if (e instanceof Error) throw new Error(`this.parseValue -> ${e.message}`)
        }

        return null
    }
}