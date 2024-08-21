import { test, expect, describe } from 'bun:test'
import Silo from '../../src/Stawrij'
import { postsURL, albumURL } from '../data'
import { mkdir, rmdir, exists } from 'node:fs/promises'

await rmdir(process.env.DB_DIR!, {recursive:true})
await mkdir(process.env.DB_DIR!, {recursive:true})

describe("NO-SQL", () => {

    const POSTS = 'posts'

    test("DROP", async () => {

        await Silo.createSchema(POSTS)

        await Silo.importBulkData<_post>(POSTS, new URL(postsURL))

        await Silo.dropSchema(POSTS)

        expect(await exists(`${process.env.DATA_PREFIX}/${POSTS}`)).toBe(false)

    })
})

describe("SQL", () => {

    const ALBUMS = 'albums'

    test("DROP", async () => {

        await Silo.executeSQL<_album>(`CREATE TABLE ${ALBUMS}`)

        await Silo.importBulkData<_album>(ALBUMS, new URL(albumURL))

        await Silo.executeSQL<_album>(`DROP TABLE ${ALBUMS}`)

        expect(await exists(`${process.env.DATA_PREFIX}/${ALBUMS}`)).toBe(false)
    })
})