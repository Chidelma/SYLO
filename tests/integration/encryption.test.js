import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import { readFile, rm } from 'node:fs/promises'
import path from 'node:path'
import Fylo from '../../src'
import { createTestRoot } from '../helpers/root'
import { CipherMock } from '../mocks/cipher'
const COLLECTION = 'encrypted-test'
const root = await createTestRoot('fylo-encryption-')
const fylo = new Fylo({ root })
mock.module('../../src/adapters/cipher', () => ({ Cipher: CipherMock }))
beforeAll(async () => {
    await fylo.createCollection(COLLECTION)
    await CipherMock.configure('test-secret-key')
    CipherMock.registerFields(COLLECTION, ['email', 'ssn', 'address'])
})
afterAll(async () => {
    CipherMock.reset()
    await fylo.dropCollection(COLLECTION)
    await rm(root, { recursive: true, force: true })
})
describe('Encryption', () => {
    let docId
    test('PUT encrypted document', async () => {
        docId = await fylo.putData(COLLECTION, {
            name: 'Alice',
            email: 'alice@example.com',
            ssn: '123-45-6789',
            age: 30
        })
        expect(docId).toBeDefined()
    })
    test('GET decrypts fields transparently', async () => {
        const result = await fylo.getDoc(COLLECTION, docId).once()
        const doc = Object.values(result)[0]
        expect(doc.name).toBe('Alice')
        expect(doc.email).toBe('alice@example.com')
        expect(doc.ssn).toBe('123-45-6789')
        expect(doc.age).toBe(30)
    })
    test('encrypted values stored in the doc file are not plaintext', async () => {
        const raw = await readFile(
            path.join(root, COLLECTION, '.fylo', 'docs', docId.slice(0, 2), `${docId}.json`),
            'utf8'
        )
        expect(raw).not.toContain('alice@example.com')
        expect(raw).not.toContain('123-45-6789')
    })
    test('$eq query works on encrypted field', async () => {
        let found = false
        for await (const data of fylo
            .findDocs(COLLECTION, {
                $ops: [{ email: { $eq: 'alice@example.com' } }]
            })
            .collect()) {
            if (typeof data === 'object') {
                const doc = Object.values(data)[0]
                expect(doc.email).toBe('alice@example.com')
                found = true
            }
        }
        expect(found).toBe(true)
    })
    test('$ne throws on encrypted field', async () => {
        try {
            const iter = fylo
                .findDocs(COLLECTION, {
                    $ops: [{ email: { $ne: 'bob@example.com' } }]
                })
                .collect()
            await iter.next()
            expect(true).toBe(false)
        } catch (e) {
            expect(e.message).toContain('not supported on encrypted field')
        }
    })
    test('$gt throws on encrypted field', async () => {
        try {
            const iter = fylo
                .findDocs(COLLECTION, {
                    $ops: [{ ssn: { $gt: 0 } }]
                })
                .collect()
            await iter.next()
            expect(true).toBe(false)
        } catch (e) {
            expect(e.message).toContain('not supported on encrypted field')
        }
    })
    test('$like throws on encrypted field', async () => {
        try {
            const iter = fylo
                .findDocs(COLLECTION, {
                    $ops: [{ email: { $like: '%@example.com' } }]
                })
                .collect()
            await iter.next()
            expect(true).toBe(false)
        } catch (e) {
            expect(e.message).toContain('not supported on encrypted field')
        }
    })
    test('non-encrypted fields remain queryable with all operators', async () => {
        let found = false
        for await (const data of fylo
            .findDocs(COLLECTION, {
                $ops: [{ name: { $eq: 'Alice' } }]
            })
            .collect()) {
            if (typeof data === 'object') {
                const doc = Object.values(data)[0]
                expect(doc.name).toBe('Alice')
                found = true
            }
        }
        expect(found).toBe(true)
    })
    test('nested encrypted field (address.city)', async () => {
        const id = await fylo.putData(COLLECTION, {
            name: 'Bob',
            email: 'bob@example.com',
            ssn: '987-65-4321',
            age: 25,
            address: { city: 'Toronto', zip: 'M5V 2T6' }
        })
        const result = await fylo.getDoc(COLLECTION, id).once()
        const doc = Object.values(result)[0]
        expect(doc.address.city).toBe('Toronto')
        expect(doc.address.zip).toBe('M5V 2T6')
    })
    test('UPDATE preserves encryption', async () => {
        await fylo.patchDoc(COLLECTION, {
            [docId]: { email: 'alice-new@example.com' }
        })
        let found = false
        for await (const data of fylo
            .findDocs(COLLECTION, {
                $ops: [{ email: { $eq: 'alice-new@example.com' } }]
            })
            .collect()) {
            if (typeof data === 'object') {
                const doc = Object.values(data)[0]
                expect(doc.email).toBe('alice-new@example.com')
                found = true
            }
        }
        expect(found).toBe(true)
    })
})
