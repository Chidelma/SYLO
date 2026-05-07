import { afterEach, describe, expect, test } from 'bun:test'
import { BunS3ClientIndexStore } from '../../src/storage/prefix-index.js'

const originalS3Client = Bun.S3Client
const originalBucketPrefix = process.env.FYLO_S3_BUCKET_PREFIX

afterEach(() => {
    Bun.S3Client = originalS3Client
    if (originalBucketPrefix === undefined) delete process.env.FYLO_S3_BUCKET_PREFIX
    else process.env.FYLO_S3_BUCKET_PREFIX = originalBucketPrefix
})

describe('Bun S3 client index', () => {
    test('maps collection names directly to bucket names', () => {
        /** @type {Array<Record<string, any>>} */
        const createdClients = []
        Bun.S3Client = class {
            /** @param {Record<string, any>} options */
            constructor(options) {
                createdClients.push(options)
            }
        }

        const store = new BunS3ClientIndexStore({ region: 'us-east-1' })
        store.client('users')

        expect(createdClients).toHaveLength(1)
        expect(createdClients[0].bucket).toBe('users')
        expect(createdClients[0].region).toBe('us-east-1')
    })

    test('does not apply legacy bucket prefix environment configuration', () => {
        /** @type {Array<Record<string, any>>} */
        const createdClients = []
        process.env.FYLO_S3_BUCKET_PREFIX = 'fylo-'
        Bun.S3Client = class {
            /** @param {Record<string, any>} options */
            constructor(options) {
                createdClients.push(options)
            }
        }

        const store = new BunS3ClientIndexStore()
        store.client('orders')

        expect(createdClients).toHaveLength(1)
        expect(createdClients[0].bucket).toBe('orders')
    })
})
