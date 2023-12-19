import { S3Client, PutObjectCommand, ListObjectsV2Command, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3'
import { executeInParallel } from '../utils/parallelum'

export class S3 {

    static async putData(client: S3Client, bucket: string, key: string, value: any) {

        try {

            await client.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: value }))

        } catch(e) {
            if(e instanceof Error) throw new Error(`S3.putData -> ${e.message}`)
        }
    }

    static async getData(client: S3Client, bucket: string, key: string) {

        let value: string = '';

        try {

            const res = await client.send(new GetObjectCommand({ Bucket: bucket, Key: key }))

            value = await res.Body!.transformToString()

        } catch(e) {
            if(e instanceof Error) throw new Error(`S3.getData -> ${e.message}`)
        }

        return value
    }

    static async getDoc(client: S3Client, bucket: string, collection: string, id: string) {

        const docs: Record<string, any> = {}

        try {

            const prefix = `${collection}/${id}`

            const keys = await this.listKeys(client, bucket, prefix)

            await executeInParallel(keys.map(async (key) => {
                docs[key] = await this.getData(client, bucket, key)
            }))

        } catch(e) {
            if(e instanceof Error) throw new Error(`S3.getDoc -> ${e.message}`)
        }

        return docs
    }

    static async delData(client: S3Client, bucket: string, key: string) {

        try {

            await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }))

        } catch(e) {
            if(e instanceof Error) throw new Error(`S3.delData -> ${e.message}`)
        }
    }

    static async delDoc(client: S3Client, bucket: string, collection: string, id: string) {

        try {

            const prefix = `${collection}/${id}`

            const keys = await this.listKeys(client, bucket, prefix)

            await executeInParallel(keys.map((key) => this.delData(client, bucket, key)))

        } catch(e) {
            if(e instanceof Error) throw new Error(`S3.delDoc -> ${e.message}`)
        }
    }

    static async listKeys(client: S3Client, bucket: string, prefix: string, max?: number) {

        let keys: string[] = []

        try {

            let token: string | undefined;

            do {

                const res = await client.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, MaxKeys: max, ContinuationToken: token }))

                token = res.ContinuationToken

                keys = [...keys, ...res.Contents!.map((content) => content.Key!)]

            } while(token !== undefined)

        } catch(e) {
            if(e instanceof Error) throw new Error(`S3.listKeys -> ${e.message}`)
        }

        return keys
    }
}