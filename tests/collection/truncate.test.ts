import { test, expect, describe } from 'bun:test'
import Silo from '../../src/Stawrij'
import { postsURL, albumURL } from '../data'
import { mkdir, rmdir } from 'node:fs/promises'

await rmdir(process.env.DB_DIR!, {recursive:true})
await mkdir(process.env.DB_DIR!, {recursive:true})

describe("NO-SQL", () => {

    const POSTS = 'posts'

    test("TRUNCATE", async () => {

        await Silo.createSchema(POSTS)

        await Silo.importBulkData<_post>(POSTS, new URL(postsURL))

        await Silo.delDocs(POSTS)

        const ids: _ulid[] = []

        for await (const data of Silo.findDocs<_post>(POSTS, { $limit: 1, $onlyIds: true }).collect()) {

            ids.push(data as _ulid)
        }

        expect(ids.length).toBe(0)

    })
})

describe("SQL", () => {

    const ALBUMS = 'albums'

    test("TRUNCATE", async () => {

        await Silo.executeSQL<_album>(`CREATE TABLE ${ALBUMS}`)

        await Silo.importBulkData<_album>(ALBUMS, new URL(albumURL))

        await Silo.executeSQL<_album>(`DELETE FROM ${ALBUMS}`)

        const ids = await Silo.executeSQL<_album>(`SELECT _id FROM ${ALBUMS} LIMIT 1`) as _ulid[]

        expect(ids.length).toBe(0)

    })
})