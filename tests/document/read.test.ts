import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Sylo from '../../src'
import { albumURL, postsURL } from '../data'

const POSTS = `post`
const ALBUMS = `album`

let count = 0

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

    await Promise.all([Sylo.createCollection(ALBUMS), sylo.executeSQL<_post>(`CREATE TABLE ${POSTS}`)])

    try {
        count = await sylo.importBulkData<_album>(ALBUMS, new URL(albumURL), 100)
        await sylo.importBulkData<_post>(POSTS, new URL(postsURL), 100)
    } catch {
        await sylo.rollback()
    }
})

afterAll(async () => {
    await Promise.all([Sylo.dropCollection(ALBUMS), Sylo.dropCollection(POSTS)])
})

describe("NO-SQL", async () => {

    test("SELECT ALL", async () => {

        let results: Record<_ttid, _album> = {}

        for await (const data of Sylo.findDocs<_album>(ALBUMS).collect()) { 

            results = { ...results, ... data as Record<_ttid, _album> }
        }

        //console.format(results)

        expect(Object.keys(results).length).toBe(count)
    })

    test("SELECT PARTIAL", async () => {

        let results: Record<_ttid, _album> = {}

        for await (const data of Sylo.findDocs<_album>(ALBUMS, { $select: ["title"] }).collect()) {

            results = { ...results, ... data as Record<_ttid, _album> }
        }

        //console.format(results)
        
        const allAlbums = Object.values(results)

        const onlyTtitle = allAlbums.every(user => user.title && !user.userId)

        expect(onlyTtitle).toBe(true)

    })

    test("GET ONE", async () => {

        const ids: _ttid[] = []

        for await (const data of Sylo.findDocs<_album>(ALBUMS, { $limit: 1, $onlyIds: true }).collect()) {

            ids.push(data as _ttid)
        }

        const result = await Sylo.getDoc<_album>(ALBUMS, ids[0]).once()
        
        const _id = Object.keys(result).shift()!

        expect(ids[0]).toEqual(_id)
    })

    test("SELECT CLAUSE", async () => {

        let results: Record<_ttid, _album> = {}

        for await (const data of Sylo.findDocs<_album>(ALBUMS, { $ops: [{ userId: { $eq: 2 } }] }).collect()) {

            results = { ...results, ...data as Record<_ttid, _album> }
        }

        //console.format(results)

        const allAlbums = Object.values(results)
        
        const onlyUserId = allAlbums.every(user => user.userId === 2)

        expect(onlyUserId).toBe(true)
    })

    test("SELECT LIMIT", async () => {

        let results: Record<_ttid, _album> = {}

        for await (const data of Sylo.findDocs<_album>(ALBUMS, { $limit: 5 }).collect()) {

            results = { ...results, ...data as Record<_ttid, _album> }
        }

        //console.format(results)

        expect(Object.keys(results).length).toBe(5)
    })

    test("SELECT GROUP BY", async () => {

        let results: Record<_album[keyof _album], Record<_ttid, Partial<_album>>> = {} as Record<_album[keyof _album], Record<_ttid, Partial<_album>>>

        for await (const data of Sylo.findDocs<_album>(ALBUMS, { $groupby: "userId", $onlyIds: true }).collect()) {
            
            results = Object.appendGroup(results, (data as unknown as Record<string, Record<string, Record<_ttid, null>>>)) 
        }

        //console.format(results)

        expect(Object.keys(results).length).toBeGreaterThan(0)
    })

    test("SELECT JOIN", async () => {

        const results = await Sylo.joinDocs<_album, _post>({ $leftCollection: ALBUMS, $rightCollection: POSTS, $mode: "inner",  $on: { "userId": { $eq: "id" } } }) as Record<`${_ttid}, ${_ttid}`, _album | _post>
        
        //console.format(results)
        
        expect(Object.keys(results).length).toBeGreaterThan(0)
    })
})

describe("SQL", async () => {

    test("SELECT PARTIAL", async () => {

        const results = await sylo.executeSQL<_album>(`SELECT title FROM ${ALBUMS}`) as Record<_ttid, _album>
        
        //console.format(results)
        
        const allAlbums = Object.values(results)
        
        const onlyTtitle = allAlbums.every(user => user.title && !user.userId)

        expect(onlyTtitle).toBe(true)
    })

    test("SELECT CLAUSE", async () => {

        const results = await sylo.executeSQL<_album>(`SELECT * FROM ${ALBUMS} WHERE user_id = 2`) as Record<_ttid, _album>
        
        //console.format(results)
        
        const allAlbums = Object.values(results)
        
        const onlyUserId = allAlbums.every(user => user.userId === 2)

        expect(onlyUserId).toBe(true)
    })

    test("SELECT ALL", async () => {

        const results = await sylo.executeSQL<_album>(`SELECT * FROM ${ALBUMS}`) as Record<_ttid, _album>
        
        //console.format(results)
        
        expect(Object.keys(results).length).toBe(count)
    })

    test("SELECT LIMIT", async () => {

        const results = await sylo.executeSQL<_album>(`SELECT * FROM ${ALBUMS} LIMIT 5`) as Record<_ttid, _album>
        
        //console.format(results)
        
        expect(Object.keys(results).length).toBe(5)
    })

    test("SELECT GROUP BY", async () => {

        const results = await sylo.executeSQL<_album>(`SELECT * FROM ${ALBUMS} GROUP BY userId`) as unknown as Record<string, Record<keyof _album, Record<_album[keyof _album], _album>>>
        
        //console.format(results)
        
        expect(Object.keys(results).length).toBeGreaterThan(0)
    })

    test("SELECT JOIN", async () => {

        const results = await sylo.executeSQL<_album>(`SELECT * FROM ${ALBUMS} INNER JOIN ${POSTS} ON userId = id`) as Record<`${_ttid}, ${_ttid}`, _album | _post>
        
        //console.format(results)
        
        expect(Object.keys(results).length).toBeGreaterThan(0)
    })
})