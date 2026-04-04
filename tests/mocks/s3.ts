/**
 * In-memory S3 mock. Replaces src/adapters/s3 so tests never touch real S3
 * or the AWS CLI. Each test file gets a fresh store because mock.module is
 * hoisted before imports, and module-level state is isolated per test file
 * in Bun's test runner.
 *
 * createBucket / deleteBucket are no-ops (bucket creation/deletion is
 * handled implicitly by the in-memory store).
 */

const store = new Map<string, Map<string, string>>()

function getBucket(name: string): Map<string, string> {
    if (!store.has(name)) store.set(name, new Map())
    return store.get(name)!
}

export default class S3Mock {

    static readonly BUCKET_ENV = process.env.BUCKET_PREFIX

    static readonly CREDS = {
        accessKeyId: 'mock',
        secretAccessKey: 'mock',
        region: 'mock',
        endpoint: undefined as string | undefined
    }

    static getBucketFormat(collection: string): string {
        return S3Mock.BUCKET_ENV ? `${S3Mock.BUCKET_ENV}-${collection}` : collection
    }

    static file(collection: string, path: string) {
        const bucket = getBucket(S3Mock.getBucketFormat(collection))
        return {
            get size() {
                const val = bucket.get(path)
                return val !== undefined ? val.length : 0
            },
            async text(): Promise<string> {
                return bucket.get(path) ?? ''
            }
        }
    }

    static async list(collection: string, options: {
        prefix?: string
        delimiter?: string
        maxKeys?: number
        continuationToken?: string
    } = {}) {
        const bucket = getBucket(S3Mock.getBucketFormat(collection))
        const prefix = options.prefix ?? ''
        const delimiter = options.delimiter
        const maxKeys = options.maxKeys ?? 1000
        const token = options.continuationToken

        const allKeys = Array.from(bucket.keys()).filter(k => k.startsWith(prefix)).sort()

        if (delimiter) {
            const prefixSet = new Set<string>()
            const contents: Array<{ key: string }> = []

            for (const key of allKeys) {
                const rest = key.slice(prefix.length)
                const idx = rest.indexOf(delimiter)
                if (idx >= 0) {
                    prefixSet.add(prefix + rest.slice(0, idx + 1))
                } else {
                    contents.push({ key })
                }
            }

            const allPrefixes = Array.from(prefixSet).map(p => ({ prefix: p }))
            const limitedPrefixes = allPrefixes.slice(0, maxKeys)

            return {
                contents: contents.length ? contents : undefined,
                commonPrefixes: limitedPrefixes.length
                    ? limitedPrefixes
                    : undefined,
                isTruncated: allPrefixes.length > maxKeys,
                nextContinuationToken: undefined
            }
        }

        const startIdx = token ? parseInt(token) : 0
        const page = allKeys.slice(startIdx, startIdx + maxKeys)
        const nextToken = startIdx + maxKeys < allKeys.length
            ? String(startIdx + maxKeys)
            : undefined

        return {
            contents: page.length ? page.map(k => ({ key: k })) : undefined,
            isTruncated: !!nextToken,
            nextContinuationToken: nextToken,
            commonPrefixes: undefined
        }
    }

    static async put(collection: string, path: string, data: string): Promise<void> {
        getBucket(S3Mock.getBucketFormat(collection)).set(path, data)
    }

    static async delete(collection: string, path: string): Promise<void> {
        getBucket(S3Mock.getBucketFormat(collection)).delete(path)
    }

    static async createBucket(_collection: string): Promise<void> {}

    static async deleteBucket(collection: string): Promise<void> {
        store.delete(S3Mock.getBucketFormat(collection))
    }
}
