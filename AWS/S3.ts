import { S3Client, PutObjectCommand, DeleteObjectCommand, ListObjectsCommand } from '@aws-sdk/client-s3'
import { _schema } from '../types/schema'


export default class {

    static async putDoc<T extends _schema<T>>(client: S3Client, bucket: string, collection: string, doc: T, deconstructDoc: (collection: string, id: string, doc: T) => string[]) {

        try {

            await Promise.all(deconstructDoc(collection, doc._id!, doc).map((key) => client.send(new PutObjectCommand({ Bucket: bucket, Key: key }))))

        } catch(e) {
            if(e instanceof Error) throw new Error(`S3.putData -> ${e.message}`)
        }
    }

    static async getDoc<T extends _schema<T>>(client: S3Client, bucket: string, collection: string, id: string, constructDoc: (keys: string[]) => T) {

        let doc: T = {} as T

        try {

            const res = await client.send(new ListObjectsCommand({ Bucket: bucket, Prefix: `${collection}/${id}` }))

            const keys = Array.from(new Set(res.Contents!.map((content) => content.Key!)))

            doc = constructDoc(keys)

        } catch(e) {
            if(e instanceof Error) throw new Error(`S3.getDoc -> ${e.message}`)
        }

        return doc
    }

    static async delDoc(client: S3Client, bucket: string, collection: string, id: string) {

        try {

            const res = await client.send(new ListObjectsCommand({ Bucket: bucket, Prefix: `${collection}/${id}` }))

            const keys = Array.from(new Set(res.Contents!.map((content) => content.Key!)))

            await Promise.all(keys.map((key) => client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }))))

        } catch(e) {
            if(e instanceof Error) throw new Error(`S3.delDoc -> ${e.message}`)
        }
    }
}