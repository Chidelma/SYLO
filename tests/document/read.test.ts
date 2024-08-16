import { test, expect, describe } from 'bun:test'
import Silo from '../../src/Stawrij'
import { albumURL, postsURL } from '../data'
import { mkdirSync, rmSync } from 'node:fs'

rmSync(process.env.DB_DIR!, {recursive:true})
mkdirSync(process.env.DB_DIR!, {recursive:true})

const ALBUMS = 'albums'
const POSTS = 'posts'

await Promise.all([Silo.createSchema(ALBUMS), Silo.createSchema(POSTS)])

describe("NO-SQL", async() => {

    const count = await Silo.importBulkData<_album>(ALBUMS, new URL(albumURL), 100)

    test("SELECT ALL", async () => {

        const results = new Map<_ulid, _album>()

        for await (const data of Silo.findDocs<_album>(ALBUMS).collect()) {

            const doc = data as Map<_ulid, _album>

            for(const [id, album] of doc) {

                results.set(id, album)
            }
        }

        expect(results.size).toBe(count)
    })

    test("SELECT PARTIAL", async () => {

        const results = new Map<_ulid, _album>()

        for await (const data of Silo.findDocs<_album>(ALBUMS, { $select: ["title"] }).collect()) {

            const doc = data as Map<_ulid, _album>

            for(const [id, album] of doc) {

                results.set(id, album)
            }
        }
        
        const allAlbums = Array.from(results.values())

        const onlyTtitle = allAlbums.every(user => user.title && !user.userId)

        expect(onlyTtitle).toBe(true)
    })

    test("GET ONE", async () => {

        const ids: _ulid[] = []

        for await (const data of Silo.findDocs<_album>(ALBUMS, { $limit: 1, $onlyIds: true }).collect()) {

            ids.push(data as _ulid)
        }

        const result = await Silo.getDoc<_album>(ALBUMS, ids[0]).once()

        const _id = Array.from(result.keys())[0]

        expect(ids[0]).toEqual(_id)
    })

    test("SELECT CLAUSE", async () => {

        const results = new Map<_ulid, _album>()

        for await (const data of Silo.findDocs<_album>(ALBUMS, { $ops: [{ userId: { $eq: 2 } }] }).collect()) {

            const doc = data as Map<_ulid, _album>

            for(const [id, album] of doc) {

                results.set(id, album)
            }
        }

        const allAlbums = Array.from(results.values())
        
        const onlyUserId = allAlbums.every(user => user.userId === 2)

        expect(onlyUserId).toBe(true)
    })

    test("SELECT LIMIT", async () => {

        const results = new Map<_ulid, _album>()

        for await (const data of Silo.findDocs<_album>(ALBUMS, { $limit: 5 }).collect()) {

            const doc = data as Map<_ulid, _album>

            for(const [id, album] of doc) {

                results.set(id, album)
            }
        }

        expect(results.size).toBe(5)
    })

    test("SELECT GROUP BY", async () => {

        const results = new Map<_album[keyof _album], _ulid[]>()

        for await (const data of Silo.findDocs<_album>(ALBUMS, { $groupby: "userId", $onlyIds: true }).collect()) {

            const doc = data as Map<_album[keyof _album], _ulid[]>

            for(const [key, ids] of doc) {

                results.set(key, ids)
            }
        }

        expect(results.size).toBeGreaterThan(0)
    })

    test("SELECT JOIN", async () => {

        const results = await Silo.joinDocs<_album, _post>({ $leftCollection: ALBUMS, $rightCollection: POSTS, $mode: "inner",  $on: { "userId": { $eq: "id" } } }) as Map<_ulid[], _album | _post>
        
        expect(results.size).toBeGreaterThan(0)
    })
})

describe("SQL", async () => {

    const count = await Silo.importBulkData<_post>(POSTS, new URL(postsURL), 100)

    test("SELECT PARTIAL", async () => {

        const results = await Silo.executeSQL<_album>(`SELECT title FROM ${ALBUMS}`) as Map<_ulid, _album>

        const allAlbums = Array.from(results.values())
        
        const onlyTtitle = allAlbums.every(user => user.title && !user.userId)

        expect(onlyTtitle).toBe(true)
    })

    test("SELECT CLAUSE", async () => {

        const results = await Silo.executeSQL<_album>(`SELECT * FROM ${ALBUMS} WHERE userId = 2`) as Map<_ulid, _album>
        
        const allAlbums = Array.from(results.values())
        
        const onlyUserId = allAlbums.every(user => user.userId === 2)

        expect(onlyUserId).toBe(true)
    })

    test("SELECT ALL", async () => {

        const results = await Silo.executeSQL<_album>(`SELECT * FROM ${ALBUMS}`) as Map<_ulid, _album>

        expect(results.size).toBe(count)
    })

    test("SELECT LIMIT", async () => {

        const results = await Silo.executeSQL<_album>(`SELECT * FROM ${ALBUMS} LIMIT 5`) as Map<_ulid, _album>

        expect(results.size).toBe(5)
    })

    test("SELECT GROUP BY", async () => {

        const results = await Silo.executeSQL<_album>(`SELECT userId FROM ${ALBUMS} GROUP BY userId`) as unknown as Map<_album[keyof _album], Map<_ulid, _album>>
        
        expect(results.size).toBeGreaterThan(0)
    })

    test("SELECT JOIN", async () => {

        const results = await Silo.executeSQL<_album>(`SELECT * FROM ${ALBUMS} INNER JOIN ${POSTS} ON userId = id`) as Map<_ulid[], _album | _post>
        
        expect(results.size).toBeGreaterThan(0)
    })
})