import { BlobServiceClient } from '@azure/storage-blob'

export default class {

    static async putDoc<T extends Record<string, any>>(client: BlobServiceClient, container: string, collection: string, id: string | number | symbol, doc: T) {

        try {

            const containerClient = client.getContainerClient(container)

            const blobClient = containerClient.getBlockBlobClient(`${collection}/${String(id)}`)

            await blobClient.uploadData(JSON.stringify(doc) as any)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Blob.putData -> ${e.message}`)
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

    static async getDoc<T extends Record<string, any>>(client: BlobServiceClient, container: string, collection: string, id: string | number | symbol) {

        let doc: T = {} as T

        try {

            const containerClient = client.getContainerClient(container)

            const blobClient = containerClient.getBlockBlobClient(`${collection}/${String(id)}`)

            const res = await blobClient.download()

            doc = JSON.parse((await this.streamToBuffer(res.readableStreamBody!)).toString())

        } catch(e) {
            if(e instanceof Error) throw new Error(`Blob.getDoc -> ${e.message}`)
        }

        return doc
    }

    static async delDoc(client: BlobServiceClient, container: string, collection: string, id: string | number | symbol) {

        try {

            const containerClient = client.getContainerClient(container)

            await containerClient.deleteBlob(`${collection}/${String(id)}`)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Blob.delDoc -> ${e.message}`)
        }
    }
}