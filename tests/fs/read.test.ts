import { test, expect, describe } from 'bun:test'
import Silo from '../../Stawrij'
import { SILO, _album, albums } from '../data'
import { mkdirSync, rmSync } from 'node:fs'

Silo.configureStorages({})

rmSync(process.env.DATA_PREFIX!, {recursive:true})
mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

const ALBUMS = 'albums'

for(const album of albums.slice(0, 25)) await Silo.putDoc(SILO, ALBUMS, album)

describe("NO-SQL", async () => {

    test("SELECT ALL", async () => {

        const results = await Silo.findDocs(ALBUMS, {}).next()

        expect(results.length).toBe(25)
    })

    test("SELECT PARTIAL", async () => {

        const results = await Silo.findDocs<_album>(ALBUMS, { $select: ["title"] }).next()

        const onlyTtitle = results.every(user => user.title && !user._id && !user.userId)

        expect(onlyTtitle).toBe(true)
    })

    test("GET ONE", async () => {

        const results = await Silo.findDocs<_album>(ALBUMS, {}).next(1)

        const result = await Silo.getDoc<_album>(ALBUMS, results[0]._id!).once()

        expect(result._id).toEqual(results[0]._id)
    })

    test("SELECT CLAUSE", async () => {

        const results = await Silo.findDocs<_album>(ALBUMS, { $ops: [{ userId: { $eq: 8 } }] }).next()

        const onlyUserId = results.every(user => user.userId === 8)

        expect(onlyUserId).toBe(true)
    })

    test("SELECT LIMIT", async () => {

        const results = await Silo.findDocs<_album>(ALBUMS, {}).next(5)

        expect(results.length).toBe(5)
    })
})

describe("SQL", () => {

    test("SELECT PARTIAL", async () => {

        const results = await Silo.findDocsSQL<_album>(`SELECT title FROM ${ALBUMS}`).next()

        const onlyTtitle = results.every(user => user.title && !user._id && !user.userId)

        expect(onlyTtitle).toBe(true)
    })

    test("SELECT CLAUSE", async () => {

        const results = await Silo.findDocsSQL<_album>(`SELECT * FROM ${ALBUMS} WHERE userId = 8`).next()

        const onlyUserId = results.every(user => user.userId === 8)

        expect(onlyUserId).toBe(true)
    })

    test("SELECT ALL", async () => {

        const results = await Silo.findDocsSQL<_album>(`SELECT * FROM ${ALBUMS}`).next()

        expect(results.length).toBe(25)
    })
})