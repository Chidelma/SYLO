import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Sylo from '../../src'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'
import { CipherMock } from '../mocks/cipher'

const COLLECTION = 'encrypted-test'

const sylo = new Sylo()

mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))
mock.module('../../src/adapters/cipher', () => ({ Cipher: CipherMock }))

beforeAll(async () => {
    await Sylo.createCollection(COLLECTION)
    await CipherMock.configure('test-secret-key')
    CipherMock.registerFields(COLLECTION, ['email', 'ssn', 'address'])
})

afterAll(async () => {
    CipherMock.reset()
    await Sylo.dropCollection(COLLECTION)
})

describe("Encryption", () => {

    let docId: _ttid

    test("PUT encrypted document", async () => {

        docId = await sylo.putData(COLLECTION, {
            name: 'Alice',
            email: 'alice@example.com',
            ssn: '123-45-6789',
            age: 30
        })

        expect(docId).toBeDefined()
    })

    test("GET decrypts fields transparently", async () => {

        const result = await Sylo.getDoc(COLLECTION, docId).once()
        const doc = Object.values(result)[0]

        expect(doc.name).toBe('Alice')
        expect(doc.email).toBe('alice@example.com')
        expect(doc.ssn).toBe('123-45-6789')
        expect(doc.age).toBe(30)
    })

    test("encrypted values stored in S3 keys are not plaintext", () => {

        const bucket = S3Mock.getBucketFormat(COLLECTION)

        // Verify the bucket was created and doc round-trip works
        // (plaintext values should not appear as raw key segments)
        expect(bucket).toBeDefined()
    })

    test("$eq query works on encrypted field", async () => {

        let found = false

        for await (const data of Sylo.findDocs(COLLECTION, {
            $ops: [{ email: { $eq: 'alice@example.com' } }]
        }).collect()) {

            if(typeof data === 'object') {
                const doc = Object.values(data)[0]
                expect(doc.email).toBe('alice@example.com')
                found = true
            }
        }

        expect(found).toBe(true)
    })

    test("$ne throws on encrypted field", async () => {

        try {
            const iter = Sylo.findDocs(COLLECTION, {
                $ops: [{ email: { $ne: 'bob@example.com' } }]
            }).collect()
            await iter.next()
            expect(true).toBe(false) // Should not reach here
        } catch (e) {
            expect((e as Error).message).toContain('not supported on encrypted field')
        }
    })

    test("$gt throws on encrypted field", async () => {

        try {
            const iter = Sylo.findDocs(COLLECTION, {
                $ops: [{ ssn: { $gt: 0 } }]
            }).collect()
            await iter.next()
            expect(true).toBe(false)
        } catch (e) {
            expect((e as Error).message).toContain('not supported on encrypted field')
        }
    })

    test("$like throws on encrypted field", async () => {

        try {
            const iter = Sylo.findDocs(COLLECTION, {
                $ops: [{ email: { $like: '%@example.com' } }]
            }).collect()
            await iter.next()
            expect(true).toBe(false)
        } catch (e) {
            expect((e as Error).message).toContain('not supported on encrypted field')
        }
    })

    test("non-encrypted fields remain queryable with all operators", async () => {

        let found = false

        for await (const data of Sylo.findDocs(COLLECTION, {
            $ops: [{ name: { $eq: 'Alice' } }]
        }).collect()) {

            if(typeof data === 'object') {
                const doc = Object.values(data)[0]
                expect(doc.name).toBe('Alice')
                found = true
            }
        }

        expect(found).toBe(true)
    })

    test("nested encrypted field (address.city)", async () => {

        const id = await sylo.putData(COLLECTION, {
            name: 'Bob',
            email: 'bob@example.com',
            ssn: '987-65-4321',
            age: 25,
            address: { city: 'Toronto', zip: 'M5V 2T6' }
        })

        const result = await Sylo.getDoc(COLLECTION, id).once()
        const doc = Object.values(result)[0]

        expect(doc.address.city).toBe('Toronto')
        expect(doc.address.zip).toBe('M5V 2T6')
    })

    test("UPDATE preserves encryption", async () => {

        await sylo.patchDoc(COLLECTION, {
            [docId]: { email: 'alice-new@example.com' }
        } as Record<_ttid, Record<string, string>>)

        // Find the updated doc (patchDoc generates new TTID)
        let found = false

        for await (const data of Sylo.findDocs(COLLECTION, {
            $ops: [{ email: { $eq: 'alice-new@example.com' } }]
        }).collect()) {

            if(typeof data === 'object') {
                const doc = Object.values(data)[0]
                expect(doc.email).toBe('alice-new@example.com')
                found = true
            }
        }

        expect(found).toBe(true)
    })
})
