import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Sylo from '../../src'
import { albumURL } from '../data'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'

/**
 * Albums from JSONPlaceholder: 100 records, userId 1–10, 10 albums per userId.
 *
 * NOTE: The numeric-range glob generator in query.ts produces digit-by-digit
 * character classes (e.g. $gt:5 → "[6-9]"). This correctly matches single-digit
 * values but misses two-digit values like 10. Tests below assert correct expected
 * values so any regression in the generator is surfaced immediately.
 */

const ALBUMS = 'ops-album'

const sylo = new Sylo()

mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))

beforeAll(async () => {
    await Sylo.createCollection(ALBUMS)
    try {
        await sylo.importBulkData<_album>(ALBUMS, new URL(albumURL), 100)
    } catch {
        await sylo.rollback()
    }
})

afterAll(async () => {
    await Sylo.dropCollection(ALBUMS)
})

describe("NO-SQL", async () => {

    test("$ne — excludes matching value", async () => {

        let results: Record<_ttid, _album> = {}

        for await (const data of Sylo.findDocs<_album>(ALBUMS, { $ops: [{ userId: { $ne: 1 } }] }).collect()) {
            results = { ...results, ...data as Record<_ttid, _album> }
        }

        const albums = Object.values(results)
        const hasUserId1 = albums.some(a => a.userId === 1)

        expect(hasUserId1).toBe(false)
        expect(albums.length).toBe(90)
    })

    test("$lt — returns documents where field is less than value", async () => {

        let results: Record<_ttid, _album> = {}

        for await (const data of Sylo.findDocs<_album>(ALBUMS, { $ops: [{ userId: { $lt: 5 } }] }).collect()) {
            results = { ...results, ...data as Record<_ttid, _album> }
        }

        const albums = Object.values(results)
        const allLessThan5 = albums.every(a => a.userId < 5)

        expect(allLessThan5).toBe(true)
        expect(albums.length).toBe(40) // userId 1,2,3,4 → 4×10
    })

    test("$lte — returns documents where field is less than or equal to value", async () => {

        let results: Record<_ttid, _album> = {}

        for await (const data of Sylo.findDocs<_album>(ALBUMS, { $ops: [{ userId: { $lte: 5 } }] }).collect()) {
            results = { ...results, ...data as Record<_ttid, _album> }
        }

        const albums = Object.values(results)
        const allLte5 = albums.every(a => a.userId <= 5)

        expect(allLte5).toBe(true)
        expect(albums.length).toBe(50) // userId 1,2,3,4,5 → 5×10
    })

    test("$gt — returns documents where field is greater than value", async () => {

        let results: Record<_ttid, _album> = {}

        for await (const data of Sylo.findDocs<_album>(ALBUMS, { $ops: [{ userId: { $gt: 5 } }] }).collect()) {
            results = { ...results, ...data as Record<_ttid, _album> }
        }

        const albums = Object.values(results)
        const allGt5 = albums.every(a => a.userId > 5)

        expect(allGt5).toBe(true)
        expect(albums.length).toBe(50) // userId 6,7,8,9,10 → 5×10
    })

    test("$gte — returns documents where field is greater than or equal to value", async () => {

        let results: Record<_ttid, _album> = {}

        for await (const data of Sylo.findDocs<_album>(ALBUMS, { $ops: [{ userId: { $gte: 5 } }] }).collect()) {
            results = { ...results, ...data as Record<_ttid, _album> }
        }

        const albums = Object.values(results)
        const allGte5 = albums.every(a => a.userId >= 5)

        expect(allGte5).toBe(true)
        expect(albums.length).toBe(60) // userId 5,6,7,8,9,10 → 6×10
    })

    test("$like — matches substring pattern", async () => {

        let results: Record<_ttid, _album> = {}

        for await (const data of Sylo.findDocs<_album>(ALBUMS, { $ops: [{ title: { $like: '%quidem%' } }] }).collect()) {
            results = { ...results, ...data as Record<_ttid, _album> }
        }

        const albums = Object.values(results)
        const allMatch = albums.every(a => a.title.includes('quidem'))

        expect(allMatch).toBe(true)
        expect(albums.length).toBeGreaterThan(0)
    })

    test("$like — prefix pattern", async () => {

        let results: Record<_ttid, _album> = {}

        for await (const data of Sylo.findDocs<_album>(ALBUMS, { $ops: [{ title: { $like: 'omnis%' } }] }).collect()) {
            results = { ...results, ...data as Record<_ttid, _album> }
        }

        const albums = Object.values(results)
        const allStartWith = albums.every(a => a.title.startsWith('omnis'))

        expect(allStartWith).toBe(true)
        expect(albums.length).toBeGreaterThan(0)
    })
})

describe("SQL", async () => {

    test("WHERE != — excludes matching value", async () => {

        const results = await sylo.executeSQL<_album>(`SELECT * FROM ${ALBUMS} WHERE userId != 1`) as Record<_ttid, _album>

        const albums = Object.values(results)
        const hasUserId1 = albums.some(a => a.userId === 1)

        expect(hasUserId1).toBe(false)
        expect(albums.length).toBe(90)
    })

    test("WHERE LIKE — matches substring pattern", async () => {

        const results = await sylo.executeSQL<_album>(`SELECT * FROM ${ALBUMS} WHERE title LIKE '%quidem%'`) as Record<_ttid, _album>

        const albums = Object.values(results)
        const allMatch = albums.every(a => a.title.includes('quidem'))

        expect(allMatch).toBe(true)
        expect(albums.length).toBeGreaterThan(0)
    })
})
