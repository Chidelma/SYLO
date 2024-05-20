import { Storage } from '@google-cloud/storage'
import { _schema } from '../types/schema'

export default class {

    static async putDoc<T extends _schema<T>>(client: Storage, bucket: string, collection: string, doc: T, deconstructDoc: (collection: string, id: string, doc: T) => string[]) {

        try {

            await Promise.all(deconstructDoc(collection, doc._id!, doc).map((key) => client.bucket(bucket).file(key).save('')))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Store.putData -> ${e.message}`)
        }
    }

    static async getDoc<T extends _schema<T>>(client: Storage, bucket: string, collection: string, id: string, constructDoc: (keys: string[]) => T) {

        let doc: T = {} as T

        try {

            const [ files ] = await client.bucket(bucket).getFiles({ prefix: `${collection}/${id}` })

            doc = constructDoc(files.map((file) => file.name))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Store.getDoc -> ${e.message}`)
        }

        return doc
    }

    static async delDoc(client: Storage, bucket: string, collection: string, id: string) {

        try {

            const [ files ] = await client.bucket(bucket).getFiles({ prefix: `${collection}/${id}` })

            await Promise.all(files.map((file) => client.bucket(bucket).file(file.name).delete()))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Store.delDoc -> ${e.message}`)
        }
    }
}