import { test, expect, describe, afterAll, beforeAll } from 'bun:test'
import Silo from '../../src/Stawrij'
import { exists, mkdir, rm } from 'node:fs/promises'
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
    await Promise.all([Silo.dropCollection(ALBUMS), Silo.dropCollection(POSTS)])
    await rm(process.env.DB_DIR!, {recursive:true})
})

describe("NO-SQL", () => {
    
    test("CREATE", async () => {

        await Silo.createCollection(POSTS)

        expect(await exists(`${process.env.DB_DIR}/${S3.getBucketFormat(POSTS)}`)).toBe(true)
    })
})

describe("SQL", () => {

    test("CREATE", async () => {

        await Silo.executeSQL<_album>(`CREATE TABLE ${ALBUMS}`)

        expect(await exists(`${process.env.DB_DIR}/${S3.getBucketFormat(ALBUMS)}`)).toBe(true)
    })
})