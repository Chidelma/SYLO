import { test, expect, describe } from 'bun:test'
import Silo from '../../src/Stawrij'
import { posts, albums } from '../data'
import { mkdirSync, rmdirSync } from 'node:fs'

rmdirSync(process.env.DATA_PREFIX!, {recursive:true})
mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

describe("NO-SQL", () => {

    const POSTS = 'posts'

    test("DROP", async () => {

        await Silo.createSchema(POSTS)

        await Silo.bulkDataPut<_post>('posts', posts.slice(0, 25))

        await Silo.truncateSchema(POSTS)

        const ids = await Silo.findDocs<_post>(POSTS, { $limit: 1, $onlyIds: true }).collect() as _uuid[]

        expect(ids.length).toBe(0)

    })
})

describe("SQL", () => {

    const ALBUMS = 'albums'

    test("DROP", async () => {

        await Silo.executeSQL<_album>(`CREATE TABLE ${ALBUMS}`)

        for(const album of albums.slice(0, 25)) {

            const keys = Object.keys(album)
            const values = Object.values(album).map(v => JSON.stringify(v))

            await Silo.executeSQL<_album>(`INSERT INTO ${ALBUMS} (${keys.join(',')}) VALUES (${values.join('\\')})`)
        }

        await Silo.executeSQL<_album>(`TRUNCATE TABLE ${ALBUMS}`)

        const cursor = await Silo.executeSQL<_album>(`SELECT _id FROM ${ALBUMS} LIMIT 1`) as _storeCursor<_album>

        const ids = await cursor.collect() as _uuid[]

        expect(ids.length).toBe(0)

    })
})