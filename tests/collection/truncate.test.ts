import { test, expect, describe, afterAll, mock } from 'bun:test'
import Sylo from '../../src'
import { postsURL, albumURL } from '../data'

const POSTS = `post`
const ALBUMS = `album`

afterAll(async () => {
    await Promise.all([Sylo.dropCollection(ALBUMS), Sylo.dropCollection(POSTS)])
})

class RedisClass {

    static async publish(collection: string, action: 'insert' | 'delete', keyId: string | _ttid) {
        
    }
}

mock.module('../../src/Redis', () => {
    return {
        default: RedisClass
    }
})

describe("NO-SQL", () => {

    test("TRUNCATE", async () => {

        const sylo = new Sylo()

        await Sylo.createCollection(POSTS)

        await sylo.importBulkData<_post>(POSTS, new URL(postsURL))

        await sylo.delDocs<_post>(POSTS)

        const ids: _ttid[] = []

        for await (const data of Sylo.findDocs<_post>(POSTS, { $limit: 1, $onlyIds: true }).collect()) {

            ids.push(data as _ttid)
        }

        expect(ids.length).toBe(0)
    })
})

describe("SQL", () => {

    test("TRUNCATE", async () => {

        const sylo = new Sylo()

        await sylo.executeSQL<_album>(`CREATE TABLE ${ALBUMS}`)

        await sylo.importBulkData<_album>(ALBUMS, new URL(albumURL))

        await sylo.executeSQL<_album>(`DELETE FROM ${ALBUMS}`)

        const ids = await sylo.executeSQL<_album>(`SELECT _id FROM ${ALBUMS} LIMIT 1`) as _ttid[]

        expect(ids.length).toBe(0)
    })
})