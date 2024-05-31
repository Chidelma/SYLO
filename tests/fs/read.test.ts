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

    test("GET ONE", async () => {
        
        const results = await Silo.findDocs<_album>(ALBUMS, { $limit: 1 })

        const result = await Silo.getDoc<_album>(ALBUMS, results[0]._id!)

        expect(result._id).toEqual(results[0]._id)
    })

    test("SELECT ALL", async () => {

        const results = await Silo.findDocs(ALBUMS, {})

        expect(results.length).toBe(25)
    })

    test("SELECT PARTIAL", async () => {

        const results = await Silo.findDocs<_album>(ALBUMS, { $select: ["title"] })

        const onlyTtitle = results.every(user => user.title && !user._id && !user.userId)

        expect(onlyTtitle).toBe(true)
    })

    test("SELECT CLAUSE", async () => {

        const results = await Silo.findDocs<_album>(ALBUMS, { $ops: [{ userId: { $eq: 8 } }] })

        const onlyUserId = results.every(user => user.userId === 8)

        expect(onlyUserId).toBe(true)
    })

    test("SELECT LIMIT", async () => {

        const results = await Silo.findDocs<_album>(ALBUMS, { $limit: 5 })

        expect(results.length).toBe(5)
    })

    test("SELECT ASC", async () => {

        const results = await Silo.findDocs<_album>(ALBUMS, { $sort: { title: 'asc' } })

        const titleIsAsc = results.every((alb, idx, arr) => idx === 0 || arr[idx - 1].title.localeCompare(alb.title) <= 0)

        expect(titleIsAsc).toBe(true)
    })

    test("SELECT DESC", async () => {

        const results = await Silo.findDocs<_album>(ALBUMS, { $sort: { title: 'desc' } })

        const titleIsDesc = results.every((alb, idx, arr) => idx === 0 || arr[idx - 1].title.localeCompare(alb.title) >= 0)

        expect(titleIsDesc).toBe(true)
    })
})

describe("SQL", () => {

    test("SELECT ALL", async () => {

        const results = await Silo.findDocsSQL<_album>(`SELECT * FROM ${ALBUMS}`)

        expect(results.length).toBe(25)
    })

    test("SELECT PARTIAL", async () => {

        const results = await Silo.findDocsSQL<_album>(`SELECT title FROM ${ALBUMS}`)

        const onlyTtitle = results.every(user => user.title && !user._id && !user.userId)

        expect(onlyTtitle).toBe(true)
    })

    test("SELECT CLAUSE", async () => {

        const results = await Silo.findDocsSQL<_album>(`SELECT * FROM ${ALBUMS} WHERE userId = 8`)

        const onlyUserId = results.every(user => user.userId === 8)

        expect(onlyUserId).toBe(true)
    })

    test("SELECT LIMIT", async () => {

        const results = await Silo.findDocsSQL<_album>(`SELECT * FROM ${ALBUMS} LIMIT 5`)

        expect(results.length).toBe(5)
    })

    test("SELECT ASC", async () => {

        const results = await Silo.findDocsSQL<_album>(`SELECT * FROM ${ALBUMS} ORDER BY TITLE ASC`)

        const titleIsAsc = results.every((alb, idx, arr) => idx === 0 || arr[idx - 1].title.localeCompare(alb.title) <= 0)

        expect(titleIsAsc).toBe(true)
    })

    test("SELECT DESC", async () => {

        const results = await Silo.findDocsSQL<_album>(`SELECT * FROM ${ALBUMS} ORDER BY TITLE DESC`)

        const titleIsDesc = results.every((alb, idx, arr) => idx === 0 || arr[idx - 1].title.localeCompare(alb.title) >= 0)

        expect(titleIsDesc).toBe(true)
    })
})