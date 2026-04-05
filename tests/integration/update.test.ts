import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Fylo from '../../src'
import { photosURL, todosURL } from '../data'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'

const PHOTOS = `photo`
const TODOS = `todo`

const fylo = new Fylo()

mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))

beforeAll(async () => {

    await Promise.all([Fylo.createCollection(PHOTOS), fylo.executeSQL<_todo>(`CREATE TABLE ${TODOS}`)])

    try {
        await fylo.importBulkData<_photo>(PHOTOS, new URL(photosURL), 100)
        await fylo.importBulkData<_todo>(TODOS, new URL(todosURL), 100)
    } catch {
        await fylo.rollback()
    }
})

afterAll(async () => {
    await Promise.all([Fylo.dropCollection(PHOTOS), fylo.executeSQL<_todo>(`DROP TABLE ${TODOS}`)])
})

describe("NO-SQL", async () => {

    test("UPDATE ONE", async () => {

        const ids: _ttid[] = []

        for await (const data of Fylo.findDocs<_photo>(PHOTOS, { $limit: 1, $onlyIds: true }).collect()) {

            ids.push(data as _ttid)
        }

        try {
            await fylo.patchDoc<_photo>(PHOTOS, { [ids.shift() as _ttid]: { title: "All Mighty" }})
        } catch {
            await fylo.rollback()
        }

        let results: Record<_ttid, _photo> = {}

        for await (const data of Fylo.findDocs<_photo>(PHOTOS, { $ops: [{ title: { $eq: "All Mighty" } }]}).collect()) {
            
            results = { ...results, ...data as Record<_ttid, _photo> }
        }

        expect(Object.keys(results).length).toBe(1)
    })

    test("UPDATE CLAUSE", async () => {

        let count = -1

        try {
            count = await fylo.patchDocs<_photo>(PHOTOS, { $set: { title: "All Mighti" }, $where: { $ops: [{ title: { $like: "%est%" } }] } })
        } catch {
            await fylo.rollback()
        }

        let results: Record<_ttid, _photo> = {}

        for await (const data of Fylo.findDocs<_photo>(PHOTOS, { $ops: [ { title: { $eq: "All Mighti" } } ] }).collect()) {

            results = { ...results, ...data as Record<_ttid, _photo> }
        }
        
        expect(Object.keys(results).length).toBe(count)
    })

    test("UPDATE ALL", async () => {

        let count = -1

        try {
            count = await fylo.patchDocs<_photo>(PHOTOS, { $set: { title: "All Mighter" } })
        } catch {
            await fylo.rollback()
        }

        let results: Record<_ttid, _photo> = {}

        for await (const data of Fylo.findDocs<_photo>(PHOTOS, { $ops: [ { title: { $eq: "All Mighter" } } ] }).collect()) {

            results = { ...results, ...data as Record<_ttid, _photo> }
        }

        expect(Object.keys(results).length).toBe(count)
    }, 20000)
})

describe("SQL", async () => {

    test("UPDATE CLAUSE", async () => {

        let count = -1
        
        try {
            count = await fylo.executeSQL<_todo>(`UPDATE ${TODOS} SET title = 'All Mighty' WHERE title LIKE '%est%'`) as number
        } catch {
            await fylo.rollback()
        }

        const results = await fylo.executeSQL<_todo>(`SELECT * FROM ${TODOS} WHERE title = 'All Mighty'`) as Record<_ttid, _todo>
        
        expect(Object.keys(results).length).toBe(count)
    })

    test("UPDATE ALL", async () => {

        let count = -1

        try {
            count = await fylo.executeSQL<_todo>(`UPDATE ${TODOS} SET title = 'All Mightier'`) as number
        } catch {
            await fylo.rollback()
        }
        
        const results = await fylo.executeSQL<_todo>(`SELECT * FROM ${TODOS} WHERE title = 'All Mightier'`) as Record<_ttid, _todo>
        
        expect(Object.keys(results).length).toBe(count)
    }, 20000)
})