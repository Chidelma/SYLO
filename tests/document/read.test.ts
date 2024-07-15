import { test, expect, describe } from 'bun:test'
import Silo from '../../src/Stawrij'
import { _album, _user, albums } from './data'
import { mkdirSync, rmSync } from 'node:fs'

rmSync(process.env.DATA_PREFIX!, {recursive:true})
mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

const ALBUMS = 'albums'

await Silo.bulkPutDocs<_album>(ALBUMS, albums.slice(0, 25))

describe("NO-SQL", async () => {

    test("SELECT ALL", async () => {

        const results = await Silo.findDocs<_album>(ALBUMS, {}).next() as Map<_uuid, _album>

        expect(results.size).toBe(25)
    })

    test("SELECT PARTIAL", async () => {

        const results = await Silo.findDocs<_album>(ALBUMS, { $select: ["title"] }).next() as Map<_uuid, _album>

        const allAlbums = Array.from(results.values())

        const onlyTtitle = allAlbums.every(user => user.title && !user.userId)

        expect(onlyTtitle).toBe(true)
    })

    test("GET ONE", async () => {

        const ids = await Silo.findDocs(ALBUMS, {}, true).next(1) as _uuid[]

        const result = await Silo.getDoc<_album>(ALBUMS, ids[0]).once()

        const id = Array.from(result.keys())[0]

        expect(ids[0]).toEqual(id)
    })

    test("SELECT CLAUSE", async () => {

        const results = await Silo.findDocs(ALBUMS, { $ops: [{ userId: { $eq: 2 } }] }).next() as Map<_uuid, _album>
        
        const allAlbums = Array.from(results.values())
        
        const onlyUserId = allAlbums.every(user => user.userId === 2)

        expect(onlyUserId).toBe(true)
    })

    test("SELECT LIMIT", async () => {

        const results = await Silo.findDocs(ALBUMS, {}).next(5) as Map<_uuid, _album>

        expect(results.size).toBe(5)
    })
})

describe("SQL", () => {

    test("SELECT PARTIAL", async () => {

        const cursor = await Silo.executeSQL<_album>(`SELECT title FROM ${ALBUMS}`) as _storeCursor<_album>

        const results = await cursor.next() as Map<_uuid, _album>

        const allAlbums = Array.from(results.values())
        
        const onlyTtitle = allAlbums.every(user => user.title && !user.userId)

        expect(onlyTtitle).toBe(true)
    })

    test("SELECT CLAUSE", async () => {

        const cursor = await Silo.executeSQL<_album>(`SELECT * FROM ${ALBUMS} WHERE userId = 2`) as _storeCursor<_album>

        const results = await cursor.next() as Map<_uuid, _album>
        
        const allAlbums = Array.from(results.values())
        
        const onlyUserId = allAlbums.every(user => user.userId === 2)

        expect(onlyUserId).toBe(true)
    })

    test("SELECT ALL", async () => {

        const cursor = await Silo.executeSQL<_album>(`SELECT * FROM ${ALBUMS}`) as _storeCursor<_album>

        const results = await cursor.next() as Map<_uuid, _album>

        expect(results.size).toBe(25)
    })
})