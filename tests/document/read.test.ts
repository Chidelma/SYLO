import { test, expect, describe } from 'bun:test'
import Silo from '../../src/Stawrij'
import { albums, posts } from '../data'
import { mkdirSync, rmSync } from 'node:fs'

rmSync(process.env.DATA_PREFIX!, {recursive:true})
mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

const ALBUMS = 'albums'

const POSTS = 'posts'

await Promise.all([Silo.createSchema(ALBUMS), Silo.createSchema(POSTS)])

await Promise.all([
    Silo.bulkDataPut<_album>(ALBUMS, albums.slice(0, 25)),
    Silo.bulkDataPut<_post>(POSTS, posts.slice(0, 25))
])

describe("NO-SQL", () => {

    test("SELECT ALL", async () => {

        const results = await Silo.findDocs<_album>(ALBUMS).collect() as Map<_uuid, _album>

        expect(results.size).toBe(25)
    })

    test("SELECT PARTIAL", async () => {

        const results = await Silo.findDocs<_album>(ALBUMS, { $select: ["title"] }).collect() as Map<_uuid, _album>

        const allAlbums = Array.from(results.values())

        const onlyTtitle = allAlbums.every(user => user.title && !user.userId)

        expect(onlyTtitle).toBe(true)
    })

    test("GET ONE", async () => {

        const ids = await Silo.findDocs<_album>(ALBUMS, { $limit: 1, $onlyIds: true }).collect() as _uuid[]

        const result = await Silo.getDoc<_album>(ALBUMS, ids[0]).once()

        const _id = Array.from(result.keys())[0]

        expect(ids[0]).toEqual(_id)
    })

    test("SELECT CLAUSE", async () => {

        const results = await Silo.findDocs<_album>(ALBUMS, { $ops: [{ userId: { $eq: 2 } }] }).collect() as Map<_uuid, _album>
        
        const allAlbums = Array.from(results.values())
        
        const onlyUserId = allAlbums.every(user => user.userId === 2)

        expect(onlyUserId).toBe(true)
    })

    test("SELECT LIMIT", async () => {

        const results = await Silo.findDocs<_album>(ALBUMS, { $limit: 5 }).collect() as Map<_uuid, _album>

        expect(results.size).toBe(5)
    })

    test("SELECT GROUP BY", async () => {

        const results = await Silo.findDocs<_album>(ALBUMS, { $groupby: "userId", $onlyIds: true }).collect() as Map<_album[keyof _album], _uuid[]>
        
        expect(results.size).toBeGreaterThan(0)
    })

    test("SELECT JOIN", async () => {

        const results = await Silo.joinDocs<_album, _post>({ $leftCollection: ALBUMS, $rightCollection: POSTS, $mode: "inner",  $on: { "userId": { $eq: "id" } } }) as Map<_uuid[], _album | _post>
        
        expect(results.size).toBeGreaterThan(0)
    })
})

describe("SQL", () => {

    test("SELECT PARTIAL", async () => {

        const cursor = await Silo.executeSQL<_album>(`SELECT title FROM ${ALBUMS}`) as _storeCursor<_album>

        const results = await cursor.collect() as Map<_uuid, _album>

        const allAlbums = Array.from(results.values())
        
        const onlyTtitle = allAlbums.every(user => user.title && !user.userId)

        expect(onlyTtitle).toBe(true)
    })

    test("SELECT CLAUSE", async () => {

        const cursor = await Silo.executeSQL<_album>(`SELECT * FROM ${ALBUMS} WHERE userId = 2`) as _storeCursor<_album>

        const results = await cursor.collect() as Map<_uuid, _album>
        
        const allAlbums = Array.from(results.values())
        
        const onlyUserId = allAlbums.every(user => user.userId === 2)

        expect(onlyUserId).toBe(true)
    })

    test("SELECT ALL", async () => {

        const cursor = await Silo.executeSQL<_album>(`SELECT * FROM ${ALBUMS}`) as _storeCursor<_album>

        const results = await cursor.collect() as Map<_uuid, _album>

        expect(results.size).toBe(25)
    })

    test("SELECT LIMIT", async () => {

        const cursor = await Silo.executeSQL<_album>(`SELECT * FROM ${ALBUMS} LIMIT 5`) as _storeCursor<_album>

        const results = await cursor.collect() as Map<_uuid, _album>

        expect(results.size).toBe(5)
    })

    test("SELECT GROUP BY", async () => {

        const cursor = await Silo.executeSQL<_album>(`SELECT userId FROM ${ALBUMS} GROUP BY userId`) as _storeCursor<_album>

        const results = await cursor.collect() as Map<_album[keyof _album], Map<_uuid, _album>>
        
        expect(results.size).toBeGreaterThan(0)
    })

    test("SELECT JOIN", async () => {

        const results = await Silo.executeSQL<_album>(`SELECT * FROM ${ALBUMS} INNER JOIN ${POSTS} ON userId = id`) as Map<_uuid[], _album | _post>
        
        expect(results.size).toBeGreaterThan(0)
    })
})