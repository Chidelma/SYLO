import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Sylo from '../../src'
import { postsURL } from '../data'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'

const POSTS = 'exp-post'
const IMPORT_LIMIT = 20

let importedCount = 0

const sylo = new Sylo()

mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))

beforeAll(async () => {
    await Sylo.createCollection(POSTS)
    try {
        importedCount = await sylo.importBulkData<_post>(POSTS, new URL(postsURL), IMPORT_LIMIT)
    } catch {
        await sylo.rollback()
    }
})

afterAll(async () => {
    await Sylo.dropCollection(POSTS)
})

describe("NO-SQL", () => {

    test("EXPORT count matches import", async () => {

        let exported = 0

        for await (const _doc of Sylo.exportBulkData<_post>(POSTS)) {
            exported++
        }

        expect(exported).toBe(importedCount)
    })

    test("EXPORT document shape", async () => {

        for await (const doc of Sylo.exportBulkData<_post>(POSTS)) {
            expect(doc).toHaveProperty('title')
            expect(doc).toHaveProperty('userId')
            expect(doc).toHaveProperty('body')
            break
        }
    })

    test("EXPORT all documents are valid posts", async () => {

        for await (const doc of Sylo.exportBulkData<_post>(POSTS)) {
            expect(typeof doc.title).toBe('string')
            expect(typeof doc.userId).toBe('number')
            expect(doc.userId).toBeGreaterThan(0)
        }
    })
})
