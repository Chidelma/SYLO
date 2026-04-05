import { $, S3Client } from "bun"

export class S3 {

    static readonly BUCKET_ENV = process.env.BUCKET_PREFIX

    static readonly CREDS = {
        accessKeyId: process.env.S3_ACCESS_KEY_ID ?? process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.S3_SECRET_ACCESS_KEY ?? process.env.AWS_SECRET_ACCESS_KEY,
        region: process.env.S3_REGION ?? process.env.AWS_REGION,
        endpoint: process.env.S3_ENDPOINT ?? process.env.AWS_ENDPOINT
    }

    private static validateCollection(collection: string): void {
        if (!/^[a-z0-9][a-z0-9\-]*[a-z0-9]$/.test(collection)) {
            throw new Error('Invalid collection name')
        }
    }

    static getBucketFormat(collection: string) {
        S3.validateCollection(collection)
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

    static async createBucket(collection: string) {
        const endpoint = S3.CREDS.endpoint
        await $`aws s3 mb s3://${S3.getBucketFormat(collection)} ${endpoint ? `--endpoint-url=${endpoint}` : ""}`.quiet()
    }

    static async deleteBucket(collection: string) {
        const endpoint = S3.CREDS.endpoint
        await $`aws s3 rm s3://${S3.getBucketFormat(collection)} --recursive ${endpoint ? `--endpoint-url=${endpoint}` : ""}`.quiet()
        await $`aws s3 rb s3://${S3.getBucketFormat(collection)} ${endpoint ? `--endpoint-url=${endpoint}` : ""}`.quiet()
    }
}
