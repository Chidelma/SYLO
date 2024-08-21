import { test, expect, describe, afterAll, beforeAll } from 'bun:test'
import Silo from '../../src/Stawrij'
import { exists, mkdir, rm } from 'node:fs/promises'

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
    
    test("CREATE", async () => {

        await Silo.createSchema(POSTS)

        const file = Bun.file(`${process.env.DB_DIR}/${POSTS}/.schema.json`)

        expect(await exists(`${process.env.DB_DIR}/${POSTS}`)).toBe(true)

        expect(await file.exists()).toBe(true)
    })
})

describe("SQL", () => {

    test("CREATE", async () => {

        await Silo.executeSQL<_album>(`CREATE TABLE ${ALBUMS}`)

        const file = Bun.file(`${process.env.DB_DIR}/${ALBUMS}/.schema.json`)

        expect(await exists(`${process.env.DB_DIR}/${ALBUMS}`)).toBe(true)

        expect(await file.exists()).toBe(true)
    })
})