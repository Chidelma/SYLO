import { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3'

export default class {

    static async putDoc(client: S3Client, bucket: string, key: string, doc: Record<string, any>) {

        try {

            await client.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: JSON.stringify(doc) }))

        } catch(e) {
            if(e instanceof Error) throw new Error(`S3.putData -> ${e.message}`)
        }
    }

    static async getDoc(client: S3Client, bucket: string, collection: string, id: string) {

        let doc: Record<string, any> = {}

        try {

            const res = await client.send(new GetObjectCommand({ Bucket: bucket, Key: `${collection}/${id}` }))

            doc = JSON.parse(await res.Body!.transformToString())

        } catch(e) {
            if(e instanceof Error) throw new Error(`S3.getDoc -> ${e.message}`)
        }

        return doc
    }

    static async delDoc(client: S3Client, bucket: string, collection: string, id: string) {

        try {

            await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: `${collection}/${id}` }))

        } catch(e) {
            if(e instanceof Error) throw new Error(`S3.delDoc -> ${e.message}`)
        }
    }
}