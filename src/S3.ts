import { S3Client } from "bun"

export default class S3 {

    static readonly BUCKET_ENV = process.env.BUCKET_PREFIX

    static readonly CREDS = {
        accessKeyId: process.env.S3_ACCESS_KEY_ID ?? process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.S3_SECRET_ACCESS_KEY ?? process.env.AWS_SECRET_ACCESS_KEY,
        region: process.env.S3_REGION ?? process.env.AWS_REGION,
        endpoint: process.env.S3_ENDPOINT ?? process.env.AWS_ENDPOINT
    }

    static getBucketFormat(collection: string) {
        return S3.BUCKET_ENV ? `${S3.BUCKET_ENV}-${collection}` : collection
    }

    static file(collection: string, path: string) {

        return S3Client.file(path, {
            bucket: S3.getBucketFormat(collection),
            ...S3.CREDS
        })
    }

    static async list(collection: string, options?: Bun.S3ListObjectsOptions) {

        return await S3Client.list(options, {
            bucket: S3.getBucketFormat(collection),
            ...S3.CREDS
        })
    }

    static async put(collection: string, path: string, data: string) {

        await S3Client.write(path, data, {
            bucket: S3.getBucketFormat(collection),
            ...S3.CREDS
        })
    }

    static async delete(collection: string, path: string) {

        await S3Client.delete(path, {
            bucket: S3.getBucketFormat(collection),
            ...S3.CREDS
        })
    }
}