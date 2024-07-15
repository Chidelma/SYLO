import { test, expect, describe } from 'bun:test'
import Silo from '../../src/Stawrij'
import { _album, _post, posts, albums } from './data'
import { mkdirSync, rmdirSync, existsSync } from 'node:fs'

rmdirSync(process.env.DATA_PREFIX!, {recursive:true})
mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

describe("NO-SQL", () => {

    const POSTS = 'posts'

    test("DROP", async () => {

        const treeItems: _treeItem<_post>[] = [{ field: 'userId' }, { field: 'title' }, { field: 'body' }]

        await Silo.createSchema<_post>(POSTS, treeItems)

        await Silo.bulkPutDocs<_post>('posts', posts.slice(0, 25))

        await Silo.dropSchema(POSTS, true)

        expect(existsSync(`${process.env.DATA_PREFIX}/${POSTS}`)).toBe(false)

    }, 60 * 60 * 1000)
})

describe("SQL", () => {

    const ALBUMS = 'albums'

    test("DROP", async () => {

        const treeItems: _treeItem<_album>[] = [{ field: 'userId' }, { field: 'title' }]

        await Silo.executeSQL<_album>(`CREATE TABLE ${ALBUMS} (${JSON.stringify(treeItems)})`)

        for(const album of albums.slice(0, 25)) {

            const keys = Object.keys(album)
            const values = Object.values(album).map(v => JSON.stringify(v))

            await Silo.executeSQL<_album>(`INSERT INTO ${ALBUMS} (${keys.join(',')}) VALUES (${values.join('\\')})`)
        }

        await Silo.executeSQL<_album>(`DROP TABLE ${ALBUMS}`)

        expect(existsSync(`${process.env.DATA_PREFIX}/${ALBUMS}`)).toBe(false)

    }, 60 * 60 * 1000)
})