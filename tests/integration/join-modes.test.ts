import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Sylo from '../../src'
import { albumURL, postsURL } from '../data'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'

/**
 * Albums (userId 1–10) and posts (userId 1–10) share a userId field,
 * making them a natural fit for join tests across all four join modes.
 *
 * Join semantics in Sylo:
 *   inner  → only the join field values
 *   left   → full left-collection document
 *   right  → full right-collection document
 *   outer  → merged left + right documents
 */

const ALBUMS = 'jm-album'
const POSTS  = 'jm-post'

const sylo = new Sylo()

mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))

beforeAll(async () => {
    await Promise.all([Sylo.createCollection(ALBUMS), Sylo.createCollection(POSTS)])
    try {
        await Promise.all([
            sylo.importBulkData<_album>(ALBUMS, new URL(albumURL), 100),
            sylo.importBulkData<_post>(POSTS, new URL(postsURL), 100)
        ])
    } catch {
        await sylo.rollback()
    }
})

afterAll(async () => {
    await Promise.all([Sylo.dropCollection(ALBUMS), Sylo.dropCollection(POSTS)])
})

describe("NO-SQL", async () => {

    test("INNER JOIN — returns only join field values", async () => {

        const results = await Sylo.joinDocs<_album, _post>({
            $leftCollection: ALBUMS,
            $rightCollection: POSTS,
            $mode: 'inner',
            $on: { userId: { $eq: 'userId' } }
        }) as Record<`${_ttid}, ${_ttid}`, { userId: number }>

        const pairs = Object.values(results)

        expect(pairs.length).toBeGreaterThan(0)

        // inner mode returns only the join fields, not full documents
        for (const pair of pairs) {
            expect(pair).toHaveProperty('userId')
            expect(typeof pair.userId).toBe('number')
        }
    })

    test("LEFT JOIN — returns full left-collection document", async () => {

        const results = await Sylo.joinDocs<_album, _post>({
            $leftCollection: ALBUMS,
            $rightCollection: POSTS,
            $mode: 'left',
            $on: { userId: { $eq: 'userId' } }
        }) as Record<`${_ttid}, ${_ttid}`, _album>

        const docs = Object.values(results)

        expect(docs.length).toBeGreaterThan(0)

        // left mode returns the full album (left collection) document
        for (const doc of docs) {
            expect(doc).toHaveProperty('title')
            expect(doc).toHaveProperty('userId')
        }
    })

    test("RIGHT JOIN — returns full right-collection document", async () => {

        const results = await Sylo.joinDocs<_album, _post>({
            $leftCollection: ALBUMS,
            $rightCollection: POSTS,
            $mode: 'right',
            $on: { userId: { $eq: 'userId' } }
        }) as Record<`${_ttid}, ${_ttid}`, _post>

        const docs = Object.values(results)

        expect(docs.length).toBeGreaterThan(0)

        // right mode returns the full post (right collection) document
        for (const doc of docs) {
            expect(doc).toHaveProperty('title')
            expect(doc).toHaveProperty('body')
            expect(doc).toHaveProperty('userId')
        }
    })

    test("OUTER JOIN — returns merged left + right document", async () => {

        const results = await Sylo.joinDocs<_album, _post>({
            $leftCollection: ALBUMS,
            $rightCollection: POSTS,
            $mode: 'outer',
            $on: { userId: { $eq: 'userId' } }
        }) as Record<`${_ttid}, ${_ttid}`, _album & _post>

        const docs = Object.values(results)

        expect(docs.length).toBeGreaterThan(0)

        // outer mode merges both documents; both should have fields present
        for (const doc of docs) {
            expect(doc).toHaveProperty('userId')
        }
    })

    test("JOIN with $limit — respects the result cap", async () => {

        const results = await Sylo.joinDocs<_album, _post>({
            $leftCollection: ALBUMS,
            $rightCollection: POSTS,
            $mode: 'inner',
            $on: { userId: { $eq: 'userId' } },
            $limit: 5
        })

        expect(Object.keys(results).length).toBe(5)
    })

    test("JOIN with $select — only requested fields are returned", async () => {

        const results = await Sylo.joinDocs<_album, _post>({
            $leftCollection: ALBUMS,
            $rightCollection: POSTS,
            $mode: 'left',
            $on: { userId: { $eq: 'userId' } },
            $select: ['title'],
            $limit: 10
        }) as Record<`${_ttid}, ${_ttid}`, Partial<_album>>

        const docs = Object.values(results)

        expect(docs.length).toBeGreaterThan(0)

        for (const doc of docs) {
            expect(doc).toHaveProperty('title')
            expect(doc).not.toHaveProperty('userId')
        }
    })

    test("JOIN with $groupby — groups results by field value", async () => {

        const results = await Sylo.joinDocs<_album, _post>({
            $leftCollection: ALBUMS,
            $rightCollection: POSTS,
            $mode: 'inner',
            $on: { userId: { $eq: 'userId' } },
            $groupby: 'userId'
        }) as Record<string, Record<string, unknown>>

        expect(Object.keys(results).length).toBeGreaterThan(0)
    })

    test("JOIN with $onlyIds — returns IDs only", async () => {

        const results = await Sylo.joinDocs<_album, _post>({
            $leftCollection: ALBUMS,
            $rightCollection: POSTS,
            $mode: 'inner',
            $on: { userId: { $eq: 'userId' } },
            $onlyIds: true,
            $limit: 10
        }) as _ttid[]

        expect(Array.isArray(results)).toBe(true)
        expect(results.length).toBeGreaterThan(0)
    })
})

describe("SQL", async () => {

    test("INNER JOIN", async () => {

        const results = await sylo.executeSQL<_album>(
            `SELECT * FROM ${ALBUMS} INNER JOIN ${POSTS} ON userId = userId`
        ) as Record<`${_ttid}, ${_ttid}`, _album | _post>

        expect(Object.keys(results).length).toBeGreaterThan(0)
    })

    test("LEFT JOIN", async () => {

        const results = await sylo.executeSQL<_album>(
            `SELECT * FROM ${ALBUMS} LEFT JOIN ${POSTS} ON userId = userId`
        ) as Record<`${_ttid}, ${_ttid}`, _album>

        const docs = Object.values(results)

        expect(docs.length).toBeGreaterThan(0)
        expect(docs[0]).toHaveProperty('title')
    })

    test("RIGHT JOIN", async () => {

        const results = await sylo.executeSQL<_post>(
            `SELECT * FROM ${ALBUMS} RIGHT JOIN ${POSTS} ON userId = userId`
        ) as Record<`${_ttid}, ${_ttid}`, _post>

        const docs = Object.values(results)

        expect(docs.length).toBeGreaterThan(0)
        expect(docs[0]).toHaveProperty('body')
    })
})
