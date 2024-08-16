import { test, expect, describe } from 'bun:test'
import Silo from '../../src/Stawrij'
import { albumURL, postsURL } from '../data'
import { mkdirSync, rmSync } from 'node:fs'

rmSync(process.env.DB_DIR!, {recursive:true})
mkdirSync(process.env.DB_DIR!, {recursive:true})

describe("NO-SQL", () => {

    const POSTS = 'posts'

    test("PUT", async () => {

        await Silo.createSchema(POSTS)

        const count = await Silo.importBulkData<_post>(POSTS, new URL(postsURL), 100)

        const results = new Map<_ulid, _post>()

        for await (const data of Silo.findDocs<_post>(POSTS).collect()) {

            const doc = data as Map<_ulid, _post>

            for(const [id, post] of doc) {

                results.set(id, post)
            }
        }

        expect(results.size).toEqual(count)
    })
})

describe("SQL", () => {

    const ALBUMS = 'albums'

    test("INSERT", async () => {

        await Silo.createSchema(ALBUMS)

        const count = await Silo.importBulkData<_album>(ALBUMS, new URL(albumURL), 100)

        const results = await Silo.executeSQL<_album>(`SELECT * FROM ${ALBUMS}`) as Map<_ulid, _album>

        expect(results.size).toEqual(count)

    })
})