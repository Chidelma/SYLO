import { test, expect, describe } from 'bun:test'
import Silo from '../src/Stawrij'
import { _album, _post, albums, posts } from './data'
import { mkdirSync, rmSync } from 'fs'
import { _storeCursor, _uuid } from '../src/types/schema'

//rmSync(process.env.DATA_PREFIX!, {recursive:true})
//mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

describe("NO-SQL", () => {

    test("PUT", async () => {

        await Silo.bulkPutDocs<_post>('posts', posts.slice(0, 25))

        const results = await Silo.findDocs('posts', {}).next() as Map<_uuid, _post>

        expect(results.size).toEqual(25)

    }, 60 * 60 * 1000)
})

const ALBUMS = 'albums'

describe("SQL", () => {

    test("INSERT", async () => {

        for(const album of albums.slice(0, 25)) {

            const keys = Object.keys(album)
            
            const params: any[] = []
            const values: any[] = []

            let count = 0

            Object.values(album).forEach(val => {
                if(typeof val === 'object') {
                    params.push(val)
                    values.push(`$${++count}`)
                } else if(typeof val === 'string') {
                    values.push(`'${val}'`)
                } else values.push(val)
            })

            await Silo.executeSQL(`INSERT INTO ${ALBUMS} (${keys.join(',')}) VALUES (${values.join(',')})`, ...params)
        }

        const cursor = await Silo.executeSQL<_album>(`SELECT * FROM ${ALBUMS}`) as _storeCursor<_album>

        const results = await cursor.next() as Map<_uuid, _album>

        expect(results.size).toEqual(25)

    }, 60 * 60 * 1000)
})