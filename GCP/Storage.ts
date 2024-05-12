import { Storage } from '@google-cloud/storage'

export default class {

    static async putDoc(client: Storage, bucket: string, key: string, doc: Record<string, any>) {

        try {

            await client.bucket(bucket).file(key).save(JSON.stringify(doc))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Store.putData -> ${e.message}`)
        }
    }

    static async getData(client: Storage, bucket: string, key: string) {

        let value: string = '';

        try {

            const res = await client.bucket(bucket).file(key).download()

            value = res.toString()

        } catch(e) {
            if(e instanceof Error) throw new Error(`Store.getData -> ${e.message}`)
        }

        return value
    }

    static async getDoc(client: Storage, bucket: string, collection: string, id: string) {

        let doc: Record<string, any> = {}

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