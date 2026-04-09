import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Fylo from '../../src'
import { albumURL, postsURL } from '../data'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'
const POSTS = `post`
const ALBUMS = `album`
let count = 0
const fylo = new Fylo()
mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))
beforeAll(async () => {
    await Promise.all([Fylo.createCollection(ALBUMS), fylo.executeSQL(`CREATE TABLE ${POSTS}`)])
    try {
        count = await fylo.importBulkData(ALBUMS, new URL(albumURL), 100)
        await fylo.importBulkData(POSTS, new URL(postsURL), 100)
    } catch {
        await fylo.rollback()
    }
})
afterAll(async () => {
    await Promise.all([Fylo.dropCollection(ALBUMS), Fylo.dropCollection(POSTS)])
})
describe('NO-SQL', async () => {
    test('SELECT ALL', async () => {
        let results = {}
        for await (const data of Fylo.findDocs(ALBUMS).collect()) {
            results = { ...results, ...data }
        }
        expect(Object.keys(results).length).toBe(count)
    })
    test('SELECT PARTIAL', async () => {
        let results = {}
        for await (const data of Fylo.findDocs(ALBUMS, { $select: ['title'] }).collect()) {
            results = { ...results, ...data }
        }
        const allAlbums = Object.values(results)
        const onlyTtitle = allAlbums.every((user) => user.title && !user.userId)
        expect(onlyTtitle).toBe(true)
    })
    test('GET ONE', async () => {
        const ids = []
        for await (const data of Fylo.findDocs(ALBUMS, { $limit: 1, $onlyIds: true }).collect()) {
            ids.push(data)
        }
        const result = await Fylo.getDoc(ALBUMS, ids[0]).once()
        const _id = Object.keys(result).shift()
        expect(ids[0]).toEqual(_id)
    })
    test('SELECT CLAUSE', async () => {
        let results = {}
        for await (const data of Fylo.findDocs(ALBUMS, {
            $ops: [{ userId: { $eq: 2 } }]
        }).collect()) {
            results = { ...results, ...data }
        }
        const allAlbums = Object.values(results)
        const onlyUserId = allAlbums.every((user) => user.userId === 2)
        expect(onlyUserId).toBe(true)
    })
    test('SELECT LIMIT', async () => {
        let results = {}
        for await (const data of Fylo.findDocs(ALBUMS, { $limit: 5 }).collect()) {
            results = { ...results, ...data }
        }
        expect(Object.keys(results).length).toBe(5)
    })
    test('SELECT GROUP BY', async () => {
        let results = {}
        for await (const data of Fylo.findDocs(ALBUMS, {
            $groupby: 'userId',
            $onlyIds: true
        }).collect()) {
            results = Object.appendGroup(results, data)
        }
        expect(Object.keys(results).length).toBeGreaterThan(0)
    })
    test('SELECT JOIN', async () => {
        const results = await Fylo.joinDocs({
            $leftCollection: ALBUMS,
            $rightCollection: POSTS,
            $mode: 'inner',
            $on: { userId: { $eq: 'id' } }
        })
        expect(Object.keys(results).length).toBeGreaterThan(0)
    })
})
describe('SQL', async () => {
    test('SELECT PARTIAL', async () => {
        const results = await fylo.executeSQL(`SELECT title FROM ${ALBUMS}`)
        const allAlbums = Object.values(results)
        const onlyTtitle = allAlbums.every((user) => user.title && !user.userId)
        expect(onlyTtitle).toBe(true)
    })
    test('SELECT CLAUSE', async () => {
        const results = await fylo.executeSQL(`SELECT * FROM ${ALBUMS} WHERE user_id = 2`)
        const allAlbums = Object.values(results)
        const onlyUserId = allAlbums.every((user) => user.userId === 2)
        expect(onlyUserId).toBe(true)
    })
    test('SELECT ALL', async () => {
        const results = await fylo.executeSQL(`SELECT * FROM ${ALBUMS}`)
        expect(Object.keys(results).length).toBe(count)
    })
    test('SELECT LIMIT', async () => {
        const results = await fylo.executeSQL(`SELECT * FROM ${ALBUMS} LIMIT 5`)
        expect(Object.keys(results).length).toBe(5)
    })
    test('SELECT GROUP BY', async () => {
        const results = await fylo.executeSQL(`SELECT * FROM ${ALBUMS} GROUP BY userId`)
        expect(Object.keys(results).length).toBeGreaterThan(0)
    })
    test('SELECT JOIN', async () => {
        const results = await fylo.executeSQL(
            `SELECT * FROM ${ALBUMS} INNER JOIN ${POSTS} ON userId = id`
        )
        expect(Object.keys(results).length).toBeGreaterThan(0)
    })
})
