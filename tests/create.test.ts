import { test, expect, describe } from 'bun:test'
import Silo from '../Stawrij'
import { _album, _post, albums, posts } from './data'
import { mkdirSync, rmSync } from 'fs'

rmSync(process.env.DATA_PREFIX!, {recursive:true})

mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

describe("NO-SQL", () => {

    test("PUT", async () => {

        await Silo.bulkPutDocs<_post>('posts', posts.slice(0, 25))

        const results = await Silo.findDocs<_post>('posts', {}).next() as _post[]

        expect(results.length).toEqual(25)

    }, 60 * 60 * 1000)
})

const ALBUMS = 'albums'

describe("SQL", () => {

    test("INSERT", async () => {

        for(const album of albums.slice(0, 25)) {
            const keys = Object.keys(album)
            const values = Object.values(album).map(val => { if(typeof val === 'string') { return `'${val}'` } else return val })
            await Silo.putDocSQL(`INSERT INTO ${ALBUMS} (${keys.join(',')}) VALUES (${values.join(',')})`)
        }

        const results = await Silo.findDocsSQL<_post>(`SELECT * FROM ${ALBUMS}`).next() as _album[]

        expect(results.length).toEqual(25)

    }, 60 * 60 * 1000)
})