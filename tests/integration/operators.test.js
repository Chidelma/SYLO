import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Fylo from '../../src'
import { albumURL } from '../data'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'
const ALBUMS = 'ops-album'
const fylo = new Fylo()
mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))
beforeAll(async () => {
    await Fylo.createCollection(ALBUMS)
    try {
        await fylo.importBulkData(ALBUMS, new URL(albumURL), 100)
    } catch {
        await fylo.rollback()
    }
})
afterAll(async () => {
    await Fylo.dropCollection(ALBUMS)
})
describe('NO-SQL', async () => {
    test('$ne — excludes matching value', async () => {
        let results = {}
        for await (const data of Fylo.findDocs(ALBUMS, {
            $ops: [{ userId: { $ne: 1 } }]
        }).collect()) {
            results = { ...results, ...data }
        }
        const albums = Object.values(results)
        const hasUserId1 = albums.some((a) => a.userId === 1)
        expect(hasUserId1).toBe(false)
        expect(albums.length).toBe(90)
    })
    test('$lt — returns documents where field is less than value', async () => {
        let results = {}
        for await (const data of Fylo.findDocs(ALBUMS, {
            $ops: [{ userId: { $lt: 5 } }]
        }).collect()) {
            results = { ...results, ...data }
        }
        const albums = Object.values(results)
        const allLessThan5 = albums.every((a) => a.userId < 5)
        expect(allLessThan5).toBe(true)
        expect(albums.length).toBe(40)
    })
    test('$lte — returns documents where field is less than or equal to value', async () => {
        let results = {}
        for await (const data of Fylo.findDocs(ALBUMS, {
            $ops: [{ userId: { $lte: 5 } }]
        }).collect()) {
            results = { ...results, ...data }
        }
        const albums = Object.values(results)
        const allLte5 = albums.every((a) => a.userId <= 5)
        expect(allLte5).toBe(true)
        expect(albums.length).toBe(50)
    })
    test('$gt — returns documents where field is greater than value', async () => {
        let results = {}
        for await (const data of Fylo.findDocs(ALBUMS, {
            $ops: [{ userId: { $gt: 5 } }]
        }).collect()) {
            results = { ...results, ...data }
        }
        const albums = Object.values(results)
        const allGt5 = albums.every((a) => a.userId > 5)
        expect(allGt5).toBe(true)
        expect(albums.length).toBe(50)
    })
    test('$gte — returns documents where field is greater than or equal to value', async () => {
        let results = {}
        for await (const data of Fylo.findDocs(ALBUMS, {
            $ops: [{ userId: { $gte: 5 } }]
        }).collect()) {
            results = { ...results, ...data }
        }
        const albums = Object.values(results)
        const allGte5 = albums.every((a) => a.userId >= 5)
        expect(allGte5).toBe(true)
        expect(albums.length).toBe(60)
    })
    test('$like — matches substring pattern', async () => {
        let results = {}
        for await (const data of Fylo.findDocs(ALBUMS, {
            $ops: [{ title: { $like: '%quidem%' } }]
        }).collect()) {
            results = { ...results, ...data }
        }
        const albums = Object.values(results)
        const allMatch = albums.every((a) => a.title.includes('quidem'))
        expect(allMatch).toBe(true)
        expect(albums.length).toBeGreaterThan(0)
    })
    test('$like — prefix pattern', async () => {
        let results = {}
        for await (const data of Fylo.findDocs(ALBUMS, {
            $ops: [{ title: { $like: 'omnis%' } }]
        }).collect()) {
            results = { ...results, ...data }
        }
        const albums = Object.values(results)
        const allStartWith = albums.every((a) => a.title.startsWith('omnis'))
        expect(allStartWith).toBe(true)
        expect(albums.length).toBeGreaterThan(0)
    })
})
describe('SQL', async () => {
    test('WHERE != — excludes matching value', async () => {
        const results = await fylo.executeSQL(`SELECT * FROM ${ALBUMS} WHERE userId != 1`)
        const albums = Object.values(results)
        const hasUserId1 = albums.some((a) => a.userId === 1)
        expect(hasUserId1).toBe(false)
        expect(albums.length).toBe(90)
    })
    test('WHERE LIKE — matches substring pattern', async () => {
        const results = await fylo.executeSQL(`SELECT * FROM ${ALBUMS} WHERE title LIKE '%quidem%'`)
        const albums = Object.values(results)
        const allMatch = albums.every((a) => a.title.includes('quidem'))
        expect(allMatch).toBe(true)
        expect(albums.length).toBeGreaterThan(0)
    })
})
