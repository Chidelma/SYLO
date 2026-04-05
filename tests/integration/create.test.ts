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

    await Promise.all([Fylo.createCollection(POSTS), fylo.executeSQL<_user>(`CREATE TABLE ${ALBUMS}`)])
    
    try {
        albumsCount = await fylo.importBulkData<_album>(ALBUMS, new URL(albumURL), 100)
        postsCount = await fylo.importBulkData<_post>(POSTS, new URL(postsURL), 100)
    } catch {
        await fylo.rollback()
    }
})

afterAll(async () => {
    await Promise.all([Fylo.dropCollection(POSTS), fylo.executeSQL<_album>(`DROP TABLE ${ALBUMS}`)])
})

describe("NO-SQL", async () => {

    test("PUT", async () => {

        let results: Record<_ttid, _post> = {}

        for await (const data of Fylo.findDocs<_post>(POSTS).collect()) {
            
            results = { ...results, ...data as Record<_ttid, _post> }
        }

        expect(Object.keys(results).length).toEqual(postsCount)
    })
})

describe("SQL", () => {

    test("INSERT", async () => {

        const results = await fylo.executeSQL<_album>(`SELECT * FROM ${ALBUMS}`) as Record<_ttid, _album>

        expect(Object.keys(results).length).toEqual(albumsCount)
    })
})