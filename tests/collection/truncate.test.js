import { test, expect, describe, afterAll, mock } from 'bun:test'
import Fylo from '../../src'
import { postsURL, albumURL } from '../data'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'
const POSTS = `post`
const ALBUMS = `album`
afterAll(async () => {
    await Promise.all([Fylo.dropCollection(ALBUMS), Fylo.dropCollection(POSTS)])
})
mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))
describe('NO-SQL', () => {
    test('TRUNCATE', async () => {
        const fylo = new Fylo()
        await Fylo.createCollection(POSTS)
        await fylo.importBulkData(POSTS, new URL(postsURL))
        await fylo.delDocs(POSTS)
        const ids = []
        for await (const data of Fylo.findDocs(POSTS, { $limit: 1, $onlyIds: true }).collect()) {
            ids.push(data)
        }
        expect(ids.length).toBe(0)
    })
})
describe('SQL', () => {
    test('TRUNCATE', async () => {
        const fylo = new Fylo()
        await fylo.executeSQL(`CREATE TABLE ${ALBUMS}`)
        await fylo.importBulkData(ALBUMS, new URL(albumURL))
        await fylo.executeSQL(`DELETE FROM ${ALBUMS}`)
        const ids = await fylo.executeSQL(`SELECT _id FROM ${ALBUMS} LIMIT 1`)
        expect(ids.length).toBe(0)
    })
})
