import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import Silo from '../../src/Stawrij'
import { albumURL, postsURL } from '../data'
import { rm, mkdir, exists } from 'node:fs/promises'

const POSTS = `posts`
const ALBUMS = `albums`

let postsCount = 0
let albumsCount = 0

beforeAll(async () => {
    if(await exists(process.env.DB_DIR!)) {
        await rm(process.env.DB_DIR!, {recursive:true})
    }
    await mkdir(process.env.DB_DIR!, {recursive:true})
    await Promise.all([Silo.createCollection(POSTS), Silo.executeSQL<_user>(`CREATE TABLE ${ALBUMS}`)])

    albumsCount = await Silo.importBulkData<_album>(ALBUMS, new URL(albumURL), 100)
    postsCount = await Silo.importBulkData<_post>(POSTS, new URL(postsURL), 100)
})

afterAll(async () => {
    await Promise.all([Silo.dropCollection(POSTS), Silo.executeSQL<_album>(`DROP TABLE ${ALBUMS}`)])
    await rm(process.env.DB_DIR!, {recursive:true})
})

describe("NO-SQL", () => {

    test("PUT", async () => {

        const results = new Map<_ulid, _post>()

        for await (const data of Silo.findDocs<_post>(POSTS).collect()) {
            
            const doc = data as Map<_ulid, _post>

            for(const [id, post] of doc) {

                results.set(id, post)
            }
        }

        expect(results.size).toEqual(postsCount)
    })
})

describe("SQL", () => {

    test("INSERT", async () => {

        const results = await Silo.executeSQL<_album>(`SELECT * FROM ${ALBUMS}`) as Map<_ulid, _album>

        expect(results.size).toEqual(albumsCount)
    })
})