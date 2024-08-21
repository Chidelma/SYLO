import { test, expect, describe } from 'bun:test'
import Silo from '../../src/Stawrij'
import { mkdir, readdir, rmdir } from 'node:fs/promises'

await rmdir(process.env.DB_DIR!, {recursive:true})
await mkdir(process.env.DB_DIR!, {recursive:true})

describe("NO-SQL", () => {

    const POSTS = 'posts'

    test("CREATE", async () => {

        await Silo.createSchema(POSTS)

        const file = Bun.file(`${process.env.DATA_PREFIX}/${POSTS}/.schema.json`)

        const savedSchema = await file.json()

        const dirKeys = await readdir(`${process.env.DATA_PREFIX}/${POSTS}`)

        const testPassed = Object.keys(savedSchema).every(key => dirKeys.includes(key))

        expect(testPassed).toBe(true)
    })
})

describe("SQL", () => {

    const ALBUMS = 'albums'

    test("CREATE", async () => {

        await Silo.executeSQL<_album>(`CREATE TABLE ${ALBUMS}`)

        const file = Bun.file(`${process.env.DATA_PREFIX}/${ALBUMS}/.schema.json`)

        const savedSchema = await file.json()

        const dirKeys = await readdir(`${process.env.DATA_PREFIX}/${ALBUMS}`)

        const testPassed = Object.keys(savedSchema).every(key => dirKeys.includes(key))

        expect(testPassed).toBe(true)
    })
})