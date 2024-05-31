import { test, expect, describe } from 'bun:test'
import Silo from '../../Stawrij'
import { SILO, _post, albums, posts } from '../data'
import { mkdirSync, rmSync } from 'fs'

Silo.configureStorages({})

rmSync(process.env.DATA_PREFIX!, {recursive:true})

mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

describe("NO-SQL", () => {

    test("PUT", async () => {

        for(const post of posts.slice(0, 25)) await Silo.putDoc(SILO, 'posts', post)

        const results = await Silo.findDocs<_post>('posts', {})

        expect(results.length).toEqual(25)
    }, 60 * 60 * 1000)
})

const ALBUMS = 'albums'

describe("SQL", () => {

    test("INSERT", async () => {

        for(const album of albums.slice(0, 25)) {
            const keys = Object.keys(album)
            const values = Object.values(album).map(val => { if(typeof val === 'string') { return `'${val}'` } else return val })
            await Silo.putDocSQL(SILO, `INSERT INTO ${ALBUMS} (${keys.join(',')}) VALUES (${values.join(',')})`)
        }

        const results = await Silo.findDocsSQL<_post>(`SELECT * FROM ${ALBUMS}`)

        expect(results.length).toEqual(25)
    }, 60 * 60 * 1000)
})