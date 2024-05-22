import { Database } from 'foundationdb'
import { _schema } from '../types/schema'

export default class {

    static async putData(db: Database, key: string) {

        await db.set(key, '')
    }

    static async putDoc<T extends _schema<T>>(db: Database, collection: string, doc: T, deconstructDoc: (collection: string, id: string, doc: T) => string[]) {

        try {

            await Promise.all(deconstructDoc(collection, doc._id!, doc).map((key) => this.putData(db, key)))

        } catch(e) {
            if(e instanceof Error) throw new Error(`S3.putDoc -> ${e.message}`)
        }
    }

    static async getColData(db: Database, prefix: string) {

        if(prefix.split(':').length === 3) throw new Error(`prefix must be a least three segments`)

        const res = await db.getRangeAllStartsWith(prefix)

        return Array.from(new Set(res.map(([key, _]) => key.toString())))
    }

    static async getDoc<T extends _schema<T>>(db: Database, collection: string, id: string, constructDoc: (keys: string[]) => T) {

        let doc: T = {} as T

        try {

            const res = await db.getRangeAllStartsWith(`${collection}/${id}`)

            doc = constructDoc(res.map(([key, _]) => key.toString()))

        } catch(e) {
            if(e instanceof Error) throw new Error(`S3.getDoc -> ${e.message}`)
        }

        return doc
    }

    static async delData(db: Database, key: string) {

        await db.clear(key)
    }

    static async delDoc(db: Database, collection: string, id: string) {

        try {

            const res = await db.getRangeAllStartsWith(`${collection}/${id}`)

            const keys = res.map(([key, _]) => key.toString())

            await Promise.all(keys.map((key) => this.delData(db, key)))

        } catch(e) {
            if(e instanceof Error) throw new Error(`S3.delDoc -> ${e.message}`)
        }
    }
}