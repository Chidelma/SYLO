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

describe("NO-SQL", () => {

    test("TRUNCATE", async () => {

        const fylo = new Fylo()

        await Fylo.createCollection(POSTS)

        await fylo.importBulkData<_post>(POSTS, new URL(postsURL))

        await fylo.delDocs<_post>(POSTS)

        const ids: _ttid[] = []

        for await (const data of Fylo.findDocs<_post>(POSTS, { $limit: 1, $onlyIds: true }).collect()) {

            ids.push(data as _ttid)
        }

        expect(ids.length).toBe(0)
    })
})

describe("SQL", () => {

    test("TRUNCATE", async () => {

        const fylo = new Fylo()

        await fylo.executeSQL<_album>(`CREATE TABLE ${ALBUMS}`)

        await fylo.importBulkData<_album>(ALBUMS, new URL(albumURL))

        await fylo.executeSQL<_album>(`DELETE FROM ${ALBUMS}`)

        const ids = await fylo.executeSQL<_album>(`SELECT _id FROM ${ALBUMS} LIMIT 1`) as _ttid[]

        expect(ids.length).toBe(0)
    })
})