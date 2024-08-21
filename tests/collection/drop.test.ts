import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import Silo from '../../src/Stawrij'
import { mkdir, exists, rm } from 'node:fs/promises'

const POSTS = 'posts'
const ALBUMS = 'albums'

beforeAll(async () => {
    await rm(process.env.DB_DIR!, {recursive:true})
    await mkdir(process.env.DB_DIR!, {recursive:true})
})

afterAll(async () => {
    await Promise.all([rm(process.env.DB_DIR!, {recursive:true}), Silo.dropSchema(ALBUMS), Silo.dropSchema(POSTS)])
})

describe("NO-SQL", () => {

    const POSTS = 'posts'

    test("DROP", async () => {

        await Silo.createSchema(POSTS)

        await Silo.dropSchema(POSTS)

        const file = Bun.file(`${process.env.DB_DIR}/${POSTS}/.schema.json`)

        expect(await file.exists()).toBe(false)

        expect(await exists(`${process.env.DB_DIR}/${POSTS}`)).toBe(false)
    })
})

describe("SQL", () => {

    const ALBUMS = 'albums'

    test("DROP", async () => {

        await Silo.executeSQL<_album>(`CREATE TABLE ${ALBUMS}`)

        await Silo.executeSQL<_album>(`DROP TABLE ${ALBUMS}`)

        const file = Bun.file(`${process.env.DB_DIR}/${ALBUMS}/.schema.json`)

        expect(await file.exists()).toBe(false)

        expect(await exists(`${process.env.DB_DIR}/${ALBUMS}`)).toBe(false)
    })
})