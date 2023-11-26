import { BlobServiceClient } from '@azure/storage-blob'

export class Blob {

    static async putData(client: BlobServiceClient, container: string, key: string, value: any) {

        try {

            const containerClient = client.getContainerClient(container)

            const blobClient = containerClient.getBlockBlobClient(key)

            await blobClient.uploadData(value)

        } catch(e) {
            if(e instanceof Error) throw new Error(e.message)
        }
    }

    private static async streamToBuffer(readableStream: NodeJS.ReadableStream) {

        return new Promise((resolve: (value: Buffer) => void, reject) => {
          
            const chunks: any[] = []

            readableStream.on("data", (data) => {
                chunks.push(data instanceof Buffer ? data : Buffer.from(data));
            })

            readableStream.on("end", () => {
                resolve(Buffer.concat(chunks))
            })

            readableStream.on("error", reject)
        })
      }

    static async getData(client: BlobServiceClient, container: string, key: string) {

        let value: string = '';

        try {

            const containerClient = client.getContainerClient(container)

            const blobClient = containerClient.getBlockBlobClient(key)

            const res = await blobClient.download()

            value = (await this.streamToBuffer(res.readableStreamBody!)).toString()

        } catch(e) {
            if(e instanceof Error) throw new Error(e.message)
        }

        return value
    }

    static async getDoc(client: BlobServiceClient, container: string, collection: string, id: string) {

        const docs: Record<string, any> = {}

        try {

            const prefix = `${collection}/${id}`

            const keys = await this.listKeys(client, container, prefix)

            await Promise.all(keys.map(async (key) => {
                docs[key] = await this.getData(client, container, key)
            }))

        } catch(e) {
            if(e instanceof Error) throw new Error(e.message)
        }

        return docs
    }

    static async delData(client: BlobServiceClient, container: string, key: string) {

        try {

            const containerClient = client.getContainerClient(container)

            await containerClient.deleteBlob(key)

        } catch(e) {
            if(e instanceof Error) throw new Error(e.message)
        }
    }

    static async delDoc(client: BlobServiceClient, container: string, collection: string, id: string) {

        try {

            const prefix = `${collection}/${id}`

            const keys = await this.listKeys(client, container, prefix)

            await Promise.all(keys.map((key) => this.delData(client, container, key)))

        } catch(e) {
            if(e instanceof Error) throw new Error(e.message)
        }
    }

    static async listKeys(client: BlobServiceClient, container: string, prefix: string) {

        let keys: string[] = []

        try {

            const containerClient = client.getContainerClient(container)

            const blobs = containerClient.listBlobsFlat({ prefix })

            for await (const blob of blobs) {
                keys.push(blob.name)
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(e.message)
        }

        return keys
    }
}