import Blob from "./Azure/Blob";
import Store from "./GCP/Storage";
import S3 from "./AWS/S3";
import { S3Client } from "@aws-sdk/client-s3";
import { BlobServiceClient } from "@azure/storage-blob";
import { Storage } from '@google-cloud/storage'
import { _storeQuery, _op } from "./types/query";
import { Glob } from 'bun'
import { mkdirSync, rmdirSync } from "node:fs";
import { watch } from 'chokidar'

export default class Stawrij {

    private s3?: S3Client
    private blob?: BlobServiceClient
    private stawr?: Storage

    private static readonly DELIMITER = '\t\b\n'

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

    async getDoc<T extends object>(silo: string, collection: string, id: string, listen?: (doc: Record<string, any>) => void) {

        let doc: T | Record<string, any> = {}

        try {

            const promises: Promise<Record<string, any>>[] = []

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

                listen(doc)

                setInterval(() => {
                    const id = Array.from(queue).shift()
                    listen(await this.getDoc(silo, collection, Array.from(queue).shift()))
                }, 5000)
                
                watch(`${collection}/**/*${id}`, { cwd: Stawrij.INDEX_PATH })
                        .on("addDir", async (path) => queue.add(path.split('/').pop()!))
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`this.getDoc -> ${e.message}`)
        }

        return doc
    }

    async putDoc<T extends Record<string, any>>(silo: string, collection: string, doc: T | Record<string, any>, idKey: string) {
        
        try {

            const promises: Promise<void>[] = []

            const record = Stawrij.unwrangleDoc<T>(doc as T, idKey)

            const id = doc[idKey]

            await this.delDocIndexes(collection, id)

            for(const key in record) mkdirSync(`${Stawrij.INDEX_PATH}/${key}`, { recursive: true })

            if(this.s3) promises.push(S3.putDoc(this.s3, silo, id, doc))
            
            if(this.blob) promises.push(Blob.putDoc(this.blob, silo, id, doc))
            
            if(this.stawr)  promises.push(Store.putDoc(this.stawr, silo, id, doc))

            await Promise.all(promises)

        } catch(e) {
            if(e instanceof Error) throw new Error(`this.putDoc -> ${e.message}`)
        }

    }

    private  async delDocIndexes(collection: string, id: string) {

        const indexes = await Array.fromAsync(new Glob(`${collection}/**/*${id}`).scan({ cwd: Stawrij.INDEX_PATH }))

        for(const idx of indexes) rmdirSync(idx, { recursive: true })
    }

    async delDoc(silo: string, collection: string, id: string) {

        try {

            const promises: Promise<void>[] = []

            if(this.s3) promises.push(S3.delDoc(this.s3, silo, collection, id))

            if(this.blob) promises.push(Blob.delDoc(this.blob, silo, collection, id))

            if(this.stawr) promises.push(Store.delDoc(this.stawr, silo, collection, id))

            await Promise.all(promises)

            await this.delDocIndexes(collection, id)

        } catch(e) {
            if(e instanceof Error) throw new Error(`this.delDoc -> ${e.message}`)
        }
    }

    private static unwrangleDoc<T>(doc: T, idKey: string, parentKey?: string) {

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

    async findDocs(silo: string, collection: string, query: _storeQuery, listen?: (docs: Record<string, any>[]) => void) {

        let results: Record<string, any>[] = []

        try {

            let count = 0

            const expressions = await this.getExprs(collection, query)

            const indexes = await Promise.all(expressions.map((expr) => Array.fromAsync(new Glob(expr).scan({ cwd: Stawrij.INDEX_PATH }))))

            results = await this.execOpIndexes(silo, collection, indexes.flat())

            results = results.filter((doc, idx, arr) => {
                idx === arr.findIndex((d) => d.id === doc.id)
            })
            
            if(query.limit) results = results.slice(0, query.limit)
            if(query.sort) {
                for(const col in query.sort) {
                    if(query.sort[col] === "asc") results.sort((a, b) => a[col].localCompare(b[col]))
                    else results.sort((a, b) => b[col].localCompare(a[col]))
                }
            }

            if(listen) {
                
                listen(results)

                setInterval(() => {
                    if(count > 0) {
                        listen(await this.findDocs(silo, collection, query))
                        count = 0
                    }
                }, 5000)

                watch(expressions, { cwd: Stawrij.INDEX_PATH })
                    .on("change", async () => count++)
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`this.findDocs -> ${e.message}`)
        }

        return results
    }

    async getExprs(collection: string, query: _storeQuery) {

        let exprs = new Set<string>()

        try {

            if(query.and) exprs = new Set([...exprs, ...await this.createAndExp(collection, query.and)])
            if(query.or) exprs = new Set([...exprs, ...await this.createOrExp(collection, query.or)])
            if(query.nor) exprs = new Set([...exprs, ...await this.createNorExp(collection, query.nor)])

        } catch(e) {
            if(e instanceof Error) throw new Error(`Silo.getExprs -> ${e.message}`)
        }

        return Array.from<string>(exprs)
    }

    private getGtOp(numbers: number[], negate: boolean = false) {

        let expression = ''

        for(const num of numbers) expression += negate ? `[!${num < 9 ? num + 1 : 9}-9]` : `[${num < 9 ? num + 1 : 9}-9]`

        return expression
    }

    private getGteOp(numbers: number[], negate: boolean = false) {

        let expression = ''

        for(const num of numbers) expression += negate ? `[!${num < 9 ? num : 9}-9]` : `[${num < 9 ? num : 9}-9]`

        return expression
    }

    private getLtOp(numbers: number[], negate: boolean = false) {

        let expression = ''

        for(const num of numbers) expression += negate ? `[!0-${num < 9 ? num - 1 : 9}]` : `[0-${num < 9 ? num - 1 : 9}]`

        return expression
    }

    private getLteOp(numbers: number[], negate: boolean = false) {

        let expression = ''

        for(const num of numbers) expression += negate ? `[!0-${num < 9 ? num : 9}]` :  `[0-${num < 9 ? num : 9}]`

        return expression
    }

    private async createAndExp(collection: string, ops: _op) {

        let globExprs: string[] = []

        try {

            const prefix = `${collection}/{${Object.keys(ops).join(',')}}`

            const valExp: string[] = []

            for(const col in ops) {

                if(ops[col].$eq) valExp.push(ops[col].$eq)
                if(ops[col].$gt) valExp.push(this.getGtOp(String(ops[col].$gt).split('').map((n) => Number(n))))
                if(ops[col].$gte) valExp.push(this.getGteOp(String(ops[col].$gte).split('').map((n) => Number(n))))
                if(ops[col].$lt) valExp.push(this.getLtOp(String(ops[col].$lt).split('').map((n) => Number(n))))
                if(ops[col].$lte) valExp.push(this.getLteOp(String(ops[col].$lte).split('').map((n) => Number(n))))
                if(ops[col].$ne) valExp.push(`!(${ops[col].$ne})`)
                if(ops[col].$like) valExp.push(ops[col].$like!)
            }

            globExprs.push(`${prefix}/{${valExp.join(',')}}/**/*`)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Silo.createAndExp -> ${e.message}`)
        }

        return globExprs
    }

    private async execOpIndexes(silo: string, collection: string, indexes: string[]) {

        let results: Record<string, any>[] = []

        try {

            const ids = indexes.map((idx) => idx.split('/').pop()!)

            results = await Promise.all(ids.map((id) => this.getDoc(silo, collection, id)))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Silo.execOpIndexes -> ${e.message}`)
        }

        return results
    }

    private async createOrExp(collection: string, ops: _op[]) {

        let globExprs: string[] = []

        try {

            for(const op of ops) {

                const prefix = `${collection}/{${Object.keys(op).join(',')}}`

                const valExp: string[] = []

                for(const col in op) {

                    if(op[col].$eq) valExp.push(op[col].$eq)
                    if(op[col].$gt) valExp.push(this.getGtOp(String(op[col].$gt).split('').map((n) => Number(n))))
                    if(op[col].$gte) valExp.push(this.getGteOp(String(op[col].$gte).split('').map((n) => Number(n))))
                    if(op[col].$lt) valExp.push(this.getLtOp(String(op[col].$lt).split('').map((n) => Number(n))))
                    if(op[col].$lte) valExp.push(this.getLteOp(String(op[col].$lte).split('').map((n) => Number(n))))
                    if(op[col].$ne) valExp.push(`!(${op[col].$ne})`)
                    if(op[col].$like) valExp.push(op[col].$like!)
                }

                globExprs.push(`${prefix}/{${valExp.join(',')}}/**/*`)
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`Silo.createOrExp -> ${e.message}`)
        }

        return globExprs
    }

    private async createNorExp(collection: string, ops: _op[]) {

        let globExprs: string[] = []

        try {

            for(const op of ops) {

                const prefix = `${collection}/{${Object.keys(op).join(',')}}`

                const valExp: string[] = []

                for(const col in op) {

                    if(op[col].$eq) valExp.push(`!(${op[col].$eq})`)
                    if(op[col].$gt) valExp.push(this.getGtOp(String(op[col].$gt).split('').map((n) => Number(n)), true))
                    if(op[col].$gte) valExp.push(this.getGteOp(String(op[col].$gte).split('').map((n) => Number(n))))
                    if(op[col].$lt) valExp.push(this.getLtOp(String(op[col].$lt).split('').map((n) => Number(n))))
                    if(op[col].$lte) valExp.push(this.getLteOp(String(op[col].$lte).split('').map((n) => Number(n))))
                    if(op[col].$ne) valExp.push(op[col].$ne)
                    if(op[col].$like) valExp.push(`!(${op[col].$like!})`)
                }

                globExprs.push(`${prefix}/{${valExp.join(',')}}/**/*`) 
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`Silo.createNorExp -> ${e.message}`)
        }

        return globExprs
    }
}