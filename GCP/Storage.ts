import { Storage } from '@google-cloud/storage'
import { _schema } from '../types/schema'

export default class {

    static async putData(client: Storage, bucket: string, key: string) {

        await client.bucket(bucket).file(key.replaceAll(':', '/')).save('')
    }

    static async putDoc<T extends _schema<T>>(client: Storage, bucket: string, collection: string, doc: T, deconstructDoc: (collection: string, id: string, doc: T) => string[]) {

        try {

            await Promise.all(deconstructDoc(collection, doc._id!, doc).map((key) => this.putData(client, bucket, key)))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Store.putData -> ${e.message}`)
        }
    }

    static async getColData(client: Storage, bucket: string, prefix: string) {

        if(prefix.split(':').length === 3) throw new Error(`prefix must be a least three segments`)

        const [ files ] = await client.bucket(bucket).getFiles({ prefix })

        return files.map((file) => file.name.replaceAll('/', ':'))
    }

    static async getDoc<T extends _schema<T>>(client: Storage, bucket: string, collection: string, id: string, constructDoc: (keys: string[]) => T) {

        let doc: T = {} as T

        try {

            const [ files ] = await client.bucket(bucket).getFiles({ prefix: `${collection}/${id}` })

            doc = constructDoc(files.map((file) => file.name.replaceAll('/', ':')))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Store.getDoc -> ${e.message}`)
        }

        return doc
    }

    static async delData(client: Storage, bucket: string, key: string) {

        await client.bucket(bucket).file(key.replaceAll(':', '/')).delete()
    }

    static async delDoc(client: Storage, bucket: string, collection: string, id: string) {

        try {

            const [ files ] = await client.bucket(bucket).getFiles({ prefix: `${collection}/${id}` })

            await Promise.all(files.map((file) => this.delData(client, bucket, file.name)))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Store.delDoc -> ${e.message}`)
        }
    }
}