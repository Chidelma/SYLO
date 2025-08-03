import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Sylo from '../../src'
import { photosURL, todosURL } from '../data'

const PHOTOS = `photo`
const TODOS = `todo`

const sylo = new Sylo()

class RedisClass {

    static async publish(collection: string, action: 'insert' | 'delete', keyId: string | _ttid) {
        
    }
}

mock.module('../../src/Redis', () => {
    return {
        default: RedisClass
    }
})

beforeAll(async () => {

    await Promise.all([Sylo.createCollection(PHOTOS), sylo.executeSQL<_todo>(`CREATE TABLE ${TODOS}`)])

    try {
        await sylo.importBulkData<_photo>(PHOTOS, new URL(photosURL), 100)
        await sylo.importBulkData<_todo>(TODOS, new URL(todosURL), 100)
    } catch {
        await sylo.rollback()
    }
})

afterAll(async () => {
    await Promise.all([Sylo.dropCollection(PHOTOS), sylo.executeSQL<_todo>(`DROP TABLE ${TODOS}`)])
})

describe("NO-SQL", async () => {

    test("UPDATE ONE", async () => {

        const ids: _ttid[] = []

        for await (const data of Sylo.findDocs<_photo>(PHOTOS, { $limit: 1, $onlyIds: true }).collect()) {

            ids.push(data as _ttid)
        }

        try {
            await sylo.patchDoc<_photo>(PHOTOS, { [ids.shift() as _ttid]: { title: "All Mighty" }})
        } catch {
            await sylo.rollback()
        }

        let results: Record<_ttid, _photo> = {}

        for await (const data of Sylo.findDocs<_photo>(PHOTOS, { $ops: [{ title: { $eq: "All Mighty" } }]}).collect()) {
            
            results = { ...results, ...data as Record<_ttid, _photo> }
        }

        expect(Object.keys(results).length).toBe(1)
    })

    test("UPDATE CLAUSE", async () => {

        let count = -1

        try {
            count = await sylo.patchDocs<_photo>(PHOTOS, { $set: { title: "All Mighti" }, $where: { $ops: [{ title: { $like: "%est%" } }] } })
        } catch {
            await sylo.rollback()
        }

        let results: Record<_ttid, _photo> = {}

        for await (const data of Sylo.findDocs<_photo>(PHOTOS, { $ops: [ { title: { $eq: "All Mighti" } } ] }).collect()) {

            results = { ...results, ...data as Record<_ttid, _photo> }
        }
        
        expect(Object.keys(results).length).toBe(count)
    })

    test("UPDATE ALL", async () => {

        let count = -1

        try {
            count = await sylo.patchDocs<_photo>(PHOTOS, { $set: { title: "All Mighter" } })
        } catch {
            await sylo.rollback()
        }

        let results: Record<_ttid, _photo> = {}

        for await (const data of Sylo.findDocs<_photo>(PHOTOS, { $ops: [ { title: { $eq: "All Mighter" } } ] }).collect()) {

            results = { ...results, ...data as Record<_ttid, _photo> }
        }

        expect(Object.keys(results).length).toBe(count)
    }, 20000)
})

describe("SQL", async () => {

    test("UPDATE CLAUSE", async () => {

        let count = -1
        
        try {
            count = await sylo.executeSQL<_todo>(`UPDATE ${TODOS} SET title = 'All Mighty' WHERE title LIKE '%est%'`) as number
        } catch {
            await sylo.rollback()
        }

        const results = await sylo.executeSQL<_todo>(`SELECT * FROM ${TODOS} WHERE title = 'All Mighty'`) as Record<_ttid, _todo>
        
        expect(Object.keys(results).length).toBe(count)
    })

    test("UPDATE ALL", async () => {

        let count = -1

        try {
            count = await sylo.executeSQL<_todo>(`UPDATE ${TODOS} SET title = 'All Mightier'`) as number
        } catch {
            await sylo.rollback()
        }
        
        const results = await sylo.executeSQL<_todo>(`SELECT * FROM ${TODOS} WHERE title = 'All Mightier'`) as Record<_ttid, _todo>
        
        expect(Object.keys(results).length).toBe(count)
    }, 20000)
})