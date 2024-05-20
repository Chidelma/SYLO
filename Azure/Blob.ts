import { BlobServiceClient } from '@azure/storage-blob'
import { _schema } from '../types/schema'

export default class {

    static async putDoc<T extends _schema<T>>(client: BlobServiceClient, container: string, collection: string, doc: T, deconstructDoc: (collection: string, is: string, doc: T) => string[]) {

        try {

            const containerClient = client.getContainerClient(container)

            await Promise.all(deconstructDoc(collection, doc._id!, doc).map((key) => containerClient.getBlockBlobClient(key).uploadData('' as any)))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Blob.putData -> ${e.message}`)
        }
    }

    static async getDoc<T extends _schema<T>>(client: BlobServiceClient, container: string, collection: string, id: string, constructDoc: (keys: string[]) => T) {

        let doc: T = {} as T

        try {

            const containerClient = client.getContainerClient(container)

            const keys: string[] = []

            for await (const blob of containerClient.listBlobsFlat({ prefix: `${collection}/${id}` })) keys.push(blob.name)

            doc = constructDoc(keys)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Blob.getDoc -> ${e.message}`)
        }

        return doc
    }

    static async delDoc(client: BlobServiceClient, container: string, collection: string, id: string) {

        try {

            const containerClient = client.getContainerClient(container)

            const keys: string[] = []

            for await (const blob of containerClient.listBlobsFlat({ prefix: `${collection}/${id}` })) keys.push(blob.name)

            await Promise.all(keys.map((key) => containerClient.getBlockBlobClient(key).delete()))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Blob.delDoc -> ${e.message}`)
        }
    }
}