import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Sylo from '../../src'
import { albumURL, postsURL } from '../data'

const POSTS = `post`
const ALBUMS = `album`

let postsCount = 0
let albumsCount = 0

const sylo = new Sylo()

class RedisClass {

    static async publish(collection: string, action: 'insert' | 'delete', keyId: string | _ttid) {
        
    }
}

mock.module('../../src/Redis', () => {
    return {
        default: RedisClass
    }
})

beforeAll(async () => {

    await Promise.all([Sylo.createCollection(POSTS), sylo.executeSQL<_user>(`CREATE TABLE ${ALBUMS}`)])
    
    try {
        albumsCount = await sylo.importBulkData<_album>(ALBUMS, new URL(albumURL), 100)
        postsCount = await sylo.importBulkData<_post>(POSTS, new URL(postsURL), 100)
    } catch {
        await sylo.rollback()
    }
})

afterAll(async () => {
    await Promise.all([Sylo.dropCollection(POSTS), sylo.executeSQL<_album>(`DROP TABLE ${ALBUMS}`)])
})

describe("NO-SQL", async () => {

    test("PUT", async () => {

        let results: Record<_ttid, _post> = {}

        for await (const data of Sylo.findDocs<_post>(POSTS).collect()) {
            
            results = { ...results, ...data as Record<_ttid, _post> }
        }

        expect(Object.keys(results).length).toEqual(postsCount)
    })
})

describe("SQL", () => {

    test("INSERT", async () => {

        const results = await sylo.executeSQL<_album>(`SELECT * FROM ${ALBUMS}`) as Record<_ttid, _album>

        expect(Object.keys(results).length).toEqual(albumsCount)
    })
})