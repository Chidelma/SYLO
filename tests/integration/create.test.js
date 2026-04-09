import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Fylo from '../../src'
import { albumURL, postsURL } from '../data'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'
const POSTS = `post`
const ALBUMS = `album`
let postsCount = 0
let albumsCount = 0
const fylo = new Fylo()
mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))
beforeAll(async () => {
    await Promise.all([Fylo.createCollection(POSTS), fylo.executeSQL(`CREATE TABLE ${ALBUMS}`)])
    try {
        albumsCount = await fylo.importBulkData(ALBUMS, new URL(albumURL), 100)
        postsCount = await fylo.importBulkData(POSTS, new URL(postsURL), 100)
    } catch {
        await fylo.rollback()
    }
})
afterAll(async () => {
    await Promise.all([Fylo.dropCollection(POSTS), fylo.executeSQL(`DROP TABLE ${ALBUMS}`)])
})
describe('NO-SQL', async () => {
    test('PUT', async () => {
        let results = {}
        for await (const data of Fylo.findDocs(POSTS).collect()) {
            results = { ...results, ...data }
        }
        expect(Object.keys(results).length).toEqual(postsCount)
    })
})
describe('SQL', () => {
    test('INSERT', async () => {
        const results = await fylo.executeSQL(`SELECT * FROM ${ALBUMS}`)
        expect(Object.keys(results).length).toEqual(albumsCount)
    })
})
