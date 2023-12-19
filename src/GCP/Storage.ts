import { Storage } from '@google-cloud/storage'
import { executeInParallel } from '../utils/parallelum'

export class Store {

    static async putData(client: Storage, bucket: string, key: string, value: any) {

        try {

            await client.bucket(bucket).file(key).save(value)

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

        const docs: Record<string, any> = {}

        try {

            const prefix = `${collection}/${id}`

            const keys = await this.listKeys(client, bucket, prefix)

            await executeInParallel(keys.map(async (key) => {
                docs[key] = await this.getData(client, bucket, key)
            }))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Store.getDoc -> ${e.message}`)
        }

        return docs
    }

    static async delData(client: Storage, bucket: string, key: string) {

        try {

            await client.bucket(bucket).file(key).delete()

        } catch(e) {
            if(e instanceof Error) throw new Error(`Store.delData -> ${e.message}`)
        }
    }

    static async delDoc(client: Storage, bucket: string, collection: string, id: string) {

        try {

            const prefix = `${collection}/${id}`

            const keys = await this.listKeys(client, bucket, prefix)

            await executeInParallel(keys.map((key) => this.delData(client, bucket, key)))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Store.delDoc -> ${e.message}`)
        }
    }

    static async listKeys(client: Storage, bucket: string, prefix: string, max?: number) {

        let keys: string[] = []

        try {

            const [ files ] = await client.bucket(bucket).getFiles({ prefix, delimiter: '/', maxResults: max })

            keys = files.map((file) => file.name)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Store.listKeys -> ${e.message}`)
        }

        return keys
    }
}