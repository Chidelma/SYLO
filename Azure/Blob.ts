import { BlobServiceClient } from '@azure/storage-blob'
import { _schema } from '../types/schema'

export default class {

    static async putData(client: BlobServiceClient, container: string, key: string) {

        const containerClient = client.getContainerClient(container)

        await containerClient.getBlockBlobClient(key.replaceAll(':', '/')).uploadData('' as any)
    }

    static async putDoc<T extends _schema<T>>(client: BlobServiceClient, container: string, collection: string, doc: T, deconstructDoc: (collection: string, is: string, doc: T) => string[]) {

        try {

            await Promise.all(deconstructDoc(collection, doc._id!, doc).map((key) => this.putData(client, container, key)))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Blob.putDoc -> ${e.message}`)
        }
    }

    static async getColData(client: BlobServiceClient, container: string, prefix: string) {

        if(prefix.split(':').length === 3) throw new Error(`prefix must be a least three segments`)

        const containerClient = client.getContainerClient(container)

        const keys: string[] = []

        for await (const blob of containerClient.listBlobsFlat({ prefix: prefix.replaceAll(':', '/') })) keys.push(blob.name)

        return keys.map(key => key.replaceAll('/', ':'))
    }

    static async getDoc<T extends _schema<T>>(client: BlobServiceClient, container: string, collection: string, id: string, constructDoc: (keys: string[]) => T) {

        let doc: T = {} as T

        try {

            const containerClient = client.getContainerClient(container)

            const keys: string[] = []

            for await (const blob of containerClient.listBlobsFlat({ prefix: `${collection}/${id}` })) keys.push(blob.name)

            doc = constructDoc(keys.map(key => key.replaceAll('/', ':')))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Blob.getDoc -> ${e.message}`)
        }

        return doc
    }

    static async delData(client: BlobServiceClient, container: string, key: string) {

        const containerClient = client.getContainerClient(container)

        await containerClient.getBlockBlobClient(key.replaceAll(':', '/')).delete()
    }

    static async delDoc(client: BlobServiceClient, container: string, collection: string, id: string) {

        try {

            const containerClient = client.getContainerClient(container)

            const keys: string[] = []

            for await (const blob of containerClient.listBlobsFlat({ prefix: `${collection}/${id}` })) keys.push(blob.name)

            await Promise.all(keys.map((key) => this.delData(client, container, key)))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Blob.delDoc -> ${e.message}`)
        }
    }
}