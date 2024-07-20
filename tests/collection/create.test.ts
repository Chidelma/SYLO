import { test, expect, describe } from 'bun:test'
import Silo from '../../src/Stawrij'
import { mkdirSync, readdirSync, rmdirSync } from 'node:fs'

rmdirSync(process.env.DATA_PREFIX!, {recursive:true})
mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

describe("NO-SQL", () => {

    const POSTS = 'posts'

    test("CREATE", async () => {

        await Silo.createSchema(POSTS)

        const file = Bun.file(`${process.env.DATA_PREFIX}/${POSTS}/.schema.json`)

        const savedSchema = await file.json()

        const dirKeys = readdirSync(`${process.env.DATA_PREFIX}/${POSTS}`)

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

        const dirKeys = readdirSync(`${process.env.DATA_PREFIX}/${ALBUMS}`)

        const testPassed = Object.keys(savedSchema).every(key => dirKeys.includes(key))

        expect(testPassed).toBe(true)
    })
})