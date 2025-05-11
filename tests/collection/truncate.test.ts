import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import Silo from '../../src/Stawrij'
import { postsURL, albumURL } from '../data'
import { exists, mkdir, rm } from 'node:fs/promises'
import S3 from '../../src/S3'

const POSTS = `posts`
const ALBUMS = `albums`

beforeAll(async () => {
    if(await exists(process.env.DB_DIR!)) {
        await rm(process.env.DB_DIR!, {recursive:true})
    }
    await mkdir(process.env.DB_DIR!, {recursive:true})
})

afterAll(async () => {
    await Promise.all([Silo.dropCollection(ALBUMS), Silo.dropCollection(POSTS)])
    await rm(process.env.DB_DIR!, {recursive:true})
})

describe("NO-SQL", () => {

    test("TRUNCATE", async () => {

        await Silo.createCollection(POSTS)

        await Silo.importBulkData<_post>(POSTS, new URL(postsURL))

        await Silo.delDocs(POSTS)

        const ids: _ulid[] = []

        for await (const data of Silo.findDocs<_post>(POSTS, { $limit: 1, $onlyIds: true }).collect()) {

            ids.push(data as _ulid)
        }

        expect(await exists(`${process.env.DB_DIR}/${S3.getBucketFormat(POSTS)}`)).toBe(true)

        expect(ids.length).toBe(0)
    })
})

describe("SQL", () => {

    test("TRUNCATE", async () => {

        await Silo.executeSQL<_album>(`CREATE TABLE ${ALBUMS}`)

        await Silo.importBulkData<_album>(ALBUMS, new URL(albumURL))

        await Silo.executeSQL<_album>(`DELETE FROM ${ALBUMS}`)

        const ids = await Silo.executeSQL<_album>(`SELECT _id FROM ${ALBUMS} LIMIT 1`) as _ulid[]

        expect(await exists(`${process.env.DB_DIR}/${S3.getBucketFormat(ALBUMS)}`)).toBe(true)

        expect(ids.length).toBe(0)
    })
})