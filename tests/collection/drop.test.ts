import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import Silo from '../../src/Stawrij'
import { mkdir, exists, rm } from 'node:fs/promises'
import { S3 } from '../../src/S3'

const POSTS = `posts`
const ALBUMS = `albums`

beforeAll(async () => {
    if(await exists(process.env.DB_DIR!)) {
        await rm(process.env.DB_DIR!, {recursive:true})
    }
    await mkdir(process.env.DB_DIR!, {recursive:true})
})

afterAll(async () => {
    await rm(process.env.DB_DIR!, {recursive:true})
})

describe("NO-SQL", () => {

    test("DROP", async () => {

        await Silo.createCollection(POSTS)

        await Silo.dropCollection(POSTS)

        expect(await exists(`${process.env.DB_DIR}/${S3.getBucketFormat(POSTS)}`)).toBe(false)
    })
})

describe("SQL", () => {

    test("DROP", async () => {

        await Silo.executeSQL<_album>(`CREATE TABLE ${ALBUMS}`)

        await Silo.executeSQL<_album>(`DROP TABLE ${ALBUMS}`)

        expect(await exists(`${process.env.DB_DIR}/${ALBUMS}`)).toBe(false)
    })
})