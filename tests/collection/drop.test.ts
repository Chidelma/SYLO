import { test, expect, describe } from 'bun:test'
import Silo from '../../src/Stawrij'
import { posts, albums } from '../data'
import { mkdirSync, rmdirSync, existsSync } from 'node:fs'

rmdirSync(process.env.DATA_PREFIX!, {recursive:true})
mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

describe("NO-SQL", () => {

    const POSTS = 'posts'

    test("DROP", async () => {

        await Silo.createSchema(POSTS)

        await Silo.bulkDataPut<_post>('posts', posts.slice(0, 25))

        Silo.dropSchema(POSTS)

        expect(existsSync(`${process.env.DATA_PREFIX}/${POSTS}`)).toBe(false)

    })
})

describe("SQL", () => {

    const ALBUMS = 'albums'

    test("DROP", async () => {

        await Silo.executeSQL<_album>(`CREATE TABLE ${ALBUMS}`)

        await Promise.all(albums.slice(0, 25).map((album: _album) => {

            const keys = Object.keys(album)
            const values = Object.values(album).map(val => JSON.stringify(val))

            return Silo.executeSQL<_album>(`INSERT INTO ${ALBUMS} (${keys.join(',')}) VALUES (${values.join('\\')})`)
        }))

        await Silo.executeSQL<_album>(`DROP TABLE ${ALBUMS}`)

        expect(existsSync(`${process.env.DATA_PREFIX}/${ALBUMS}`)).toBe(false)

    })
})