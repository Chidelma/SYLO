import { Storage } from '@google-cloud/storage'

export default class {

    static async putDoc<T extends Record<string, any>>(client: Storage, bucket: string, key: string, doc: T) {

        try {

            await client.bucket(bucket).file(key).save(JSON.stringify(doc))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Store.putData -> ${e.message}`)
        }
    }

    static async getDoc<T>(client: Storage, bucket: string, collection: string, id: string) {

        let doc: T = {} as T

        try {

            const res = await client.bucket(bucket).file(`${collection}/${id}`).download()

            doc = JSON.parse(res.toString())

        } catch(e) {
            if(e instanceof Error) throw new Error(`Store.getDoc -> ${e.message}`)
        }

        return doc
    }

    static async delDoc(client: Storage, bucket: string, collection: string, id: string) {

        try {

            await client.bucket(bucket).file(`${collection}/${id}`).delete()

        } catch(e) {
            if(e instanceof Error) throw new Error(`Store.delDoc -> ${e.message}`)
        }
    }
}