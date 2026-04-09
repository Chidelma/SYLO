const store = new Map()
function getBucket(name) {
    if (!store.has(name)) store.set(name, new Map())
    return store.get(name)
}
export default class S3Mock {
    static BUCKET_ENV = process.env.BUCKET_PREFIX
    static CREDS = {
        accessKeyId: 'mock',
        secretAccessKey: 'mock',
        region: 'mock',
        endpoint: undefined
    }
    static getBucketFormat(collection) {
        return S3Mock.BUCKET_ENV ? `${S3Mock.BUCKET_ENV}-${collection}` : collection
    }
    static file(collection, path) {
        const bucket = getBucket(S3Mock.getBucketFormat(collection))
        return {
            get size() {
                const val = bucket.get(path)
                return val !== undefined ? val.length : 0
            },
            async text() {
                return bucket.get(path) ?? ''
            }
        }
    }
    static async list(collection, options = {}) {
        const bucket = getBucket(S3Mock.getBucketFormat(collection))
        const prefix = options.prefix ?? ''
        const delimiter = options.delimiter
        const maxKeys = options.maxKeys ?? 1000
        const token = options.continuationToken
        const allKeys = Array.from(bucket.keys())
            .filter((k) => k.startsWith(prefix))
            .sort()
        if (delimiter) {
            const prefixSet = new Set()
            const contents = []
            for (const key of allKeys) {
                const rest = key.slice(prefix.length)
                const idx = rest.indexOf(delimiter)
                if (idx >= 0) {
                    prefixSet.add(prefix + rest.slice(0, idx + 1))
                } else {
                    contents.push({ key })
                }
            }
            const allPrefixes = Array.from(prefixSet).map((p) => ({ prefix: p }))
            const limitedPrefixes = allPrefixes.slice(0, maxKeys)
            return {
                contents: contents.length ? contents : undefined,
                commonPrefixes: limitedPrefixes.length ? limitedPrefixes : undefined,
                isTruncated: allPrefixes.length > maxKeys,
                nextContinuationToken: undefined
            }
        }
        const startIdx = token ? parseInt(token) : 0
        const page = allKeys.slice(startIdx, startIdx + maxKeys)
        const nextToken =
            startIdx + maxKeys < allKeys.length ? String(startIdx + maxKeys) : undefined
        return {
            contents: page.length ? page.map((k) => ({ key: k })) : undefined,
            isTruncated: !!nextToken,
            nextContinuationToken: nextToken,
            commonPrefixes: undefined
        }
    }
    static async put(collection, path, data) {
        getBucket(S3Mock.getBucketFormat(collection)).set(path, data)
    }
    static async delete(collection, path) {
        getBucket(S3Mock.getBucketFormat(collection)).delete(path)
    }
    static async createBucket(_collection) {}
    static async deleteBucket(collection) {
        store.delete(S3Mock.getBucketFormat(collection))
    }
}
