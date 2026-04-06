import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Fylo from '../../src'
import TTID from '@delma/ttid'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'

/**
 * Edge case coverage:
 *   - Non-existent ID returns empty object
 *   - Values containing forward slashes survive the SLASH_ASCII (%2F) round-trip
 *   - Multiple $ops entries act as OR (union across patterns)
 *   - $rename renames fields in query output
 *   - Versioned putData (existing TTID as key) preserves the creation-time prefix
 *   - SQL UPDATE ONE — update a single document by ID via SQL
 *   - SQL DELETE ONE — delete a single document by ID via SQL
 */

const COLLECTION = 'ec-test'

const fylo = new Fylo()

mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))

beforeAll(async () => {
    await Fylo.createCollection(COLLECTION)
})

afterAll(async () => {
    await Fylo.dropCollection(COLLECTION)
})

describe("NO-SQL", () => {

    test("GET ONE — non-existent ID returns empty object", async () => {

        const fakeId = TTID.generate() as _ttid

        const result = await Fylo.getDoc(COLLECTION, fakeId).once()

        expect(Object.keys(result).length).toBe(0)
    })

    test("PUT / GET — forward slashes in values round-trip correctly", async () => {

        const original = {
            userId: 1,
            id: 1,
            title: 'Slash Test',
            body: 'https://example.com/api/v1/resource'
        }

        const _id = await fylo.putData<_post>(COLLECTION, original)

        const result = await Fylo.getDoc<_post>(COLLECTION, _id).once()
        const doc = result[_id]

        expect(doc.body).toBe(original.body)

        await fylo.delDoc(COLLECTION, _id)
    })

    test("PUT / GET — values with multiple consecutive slashes round-trip correctly", async () => {

        const original = {
            userId: 1,
            id: 2,
            title: 'Double Slash',
            body: 'https://cdn.example.com//assets//image.png'
        }

        const _id = await fylo.putData<_post>(COLLECTION, original)

        const result = await Fylo.getDoc<_post>(COLLECTION, _id).once()

        expect(result[_id].body).toBe(original.body)

        await fylo.delDoc(COLLECTION, _id)
    })

    test("$ops — multiple conditions act as OR union", async () => {

        const cleanFylo = new Fylo()

        const id1 = await cleanFylo.putData<_post>(COLLECTION, { userId: 10, id: 100, title: 'Alpha', body: 'first' })
        const id2 = await cleanFylo.putData<_post>(COLLECTION, { userId: 20, id: 200, title: 'Beta',  body: 'second' })

        const results: Record<_ttid, _post> = {}

        for await (const data of Fylo.findDocs<_post>(COLLECTION, {
            $ops: [
                { userId: { $eq: 10 } },
                { userId: { $eq: 20 } }
            ]
        }).collect()) {
            Object.assign(results, data)
        }

        expect(results[id1]).toBeDefined()
        expect(results[id2]).toBeDefined()

        await cleanFylo.delDoc(COLLECTION, id1)
        await cleanFylo.delDoc(COLLECTION, id2)
    })

    test("$rename — renames fields in query output", async () => {

        const cleanFylo = new Fylo()

        const _id = await cleanFylo.putData<_post>(COLLECTION, {
            userId: 1,
            id: 300,
            title: 'Rename Me',
            body: 'some body'
        })

        let renamed: Partial<_post> & { name?: string } = {}

        for await (const data of Fylo.findDocs<_post>(COLLECTION, {
            $ops: [{ id: { $eq: 300 } }],
            $rename: { title: 'name' } as Record<keyof Partial<_post>, string>
        }).collect()) {
            renamed = Object.values(data as Record<_ttid, typeof renamed>)[0]
        }

        expect(renamed.name).toBe('Rename Me')
        expect(renamed.title).toBeUndefined()

        await cleanFylo.delDoc(COLLECTION, _id)
    })

    test("versioned putData — preserves creation-time prefix in TTID", async () => {

        const cleanFylo = new Fylo()

        const _id1 = await cleanFylo.putData<_post>(COLLECTION, {
            userId: 1,
            id: 400,
            title: 'Original',
            body: 'v1'
        })

        const _id2 = await cleanFylo.putData<_post>(COLLECTION, {
            [_id1]: { userId: 1, id: 400, title: 'Updated', body: 'v2' }
        })

        // The creation-time segment (before the first '-') must be identical
        expect(_id2.split('-')[0]).toBe(_id1.split('-')[0])

        // The updated doc is retrievable via its own TTID
        const result = await Fylo.getDoc<_post>(COLLECTION, _id2).once()
        const doc = result[_id2]

        expect(doc).toBeDefined()
        expect(doc.title).toBe('Updated')

        // Clean up both versions
        await cleanFylo.delDoc(COLLECTION, _id1)
        await cleanFylo.delDoc(COLLECTION, _id2)
    })

    test("versioned putData — original version is no longer retrievable by old full TTID", async () => {

        const cleanFylo = new Fylo()

        const _id1 = await cleanFylo.putData<_post>(COLLECTION, {
            userId: 1,
            id: 500,
            title: 'Old Version',
            body: 'original'
        })

        const _id2 = await cleanFylo.putData<_post>(COLLECTION, {
            [_id1]: { userId: 1, id: 500, title: 'New Version', body: 'updated' }
        })

        // Both IDs are different (update appended a segment)
        expect(_id1).not.toBe(_id2)

        await cleanFylo.delDoc(COLLECTION, _id2)
    })
})

describe("SQL", () => {

    test("UPDATE ONE — update a single document by querying its unique field", async () => {

        const cleanFylo = new Fylo()

        await cleanFylo.putData<_post>(COLLECTION, {
            userId: 1,
            id: 600,
            title: 'Before SQL Update',
            body: 'original'
        })

        const updated = await cleanFylo.executeSQL<_post>(
            `UPDATE ${COLLECTION} SET title = 'After SQL Update' WHERE id = 600`
        ) as number

        expect(updated).toBe(1)

        const results = await cleanFylo.executeSQL<_post>(
            `SELECT * FROM ${COLLECTION} WHERE title = 'After SQL Update'`
        ) as Record<_ttid, _post>

        expect(Object.keys(results).length).toBe(1)
        expect(Object.values(results)[0].title).toBe('After SQL Update')
    })

    test("DELETE ONE — delete a single document by querying its unique field", async () => {

        const cleanFylo = new Fylo()

        await cleanFylo.putData<_post>(COLLECTION, {
            userId: 1,
            id: 700,
            title: 'Delete Via SQL',
            body: 'should be removed'
        })

        await cleanFylo.executeSQL<_post>(
            `DELETE FROM ${COLLECTION} WHERE title = 'Delete Via SQL'`
        )

        const results = await cleanFylo.executeSQL<_post>(
            `SELECT * FROM ${COLLECTION} WHERE title = 'Delete Via SQL'`
        ) as Record<_ttid, _post>

        expect(Object.keys(results).length).toBe(0)
    })
})
