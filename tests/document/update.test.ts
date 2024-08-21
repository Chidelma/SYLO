import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import Silo from '../../src/Stawrij'
import { photosURL, todosURL } from '../data'
import { mkdir, rm } from 'node:fs/promises' 

const PHOTOS = 'photos'
const TODOS = 'todos'

beforeAll(async () => {
    await rm(process.env.DB_DIR!, {recursive:true})
    await mkdir(process.env.DB_DIR!, {recursive:true})
    await Promise.all([Silo.createSchema(PHOTOS), Silo.executeSQL<_todo>(`CREATE TABLE ${TODOS}`)])

    await Silo.importBulkData<_photo>(PHOTOS, new URL(photosURL), 100)
    await Silo.importBulkData<_todo>(TODOS, new URL(todosURL), 100)
})

afterAll(async () => {
    await Promise.all([rm(process.env.DB_DIR!, {recursive:true}), Silo.dropSchema(PHOTOS), Silo.executeSQL<_todo>(`DROP TABLE ${TODOS}`)])
})

describe("NO-SQL", async () => {

    test("UPDATE ONE", async () => {

        const ids: _ulid[] = []

        for await (const data of Silo.findDocs<_photo>(PHOTOS, { $limit: 1, $onlyIds: true }).collect()) {

            ids.push(data as _ulid)
        }

        await Silo.patchDoc<_photo>(PHOTOS, new Map([[ids[0], { title: "All Mighty" }]]))

        const results = new Map<_ulid, _photo>()

        for await (const data of Silo.findDocs<_photo>(PHOTOS, { $ops: [{ title: { $eq: "All Mighty" } }]}).collect()) {

            const doc = data as Map<_ulid, _photo>

            for(const [id, photo] of doc) {

                results.set(id, photo)
            }
        }

        expect(results.size).toBe(1)
    })

    test("UPDATE CLAUSE", async () => {

        const count = await Silo.patchDocs<_photo>(PHOTOS, { $set: { title: "All Mighti" }, $where: { $ops: [{ title: { $like: "%est%" } }] } })

        const results = new Map<_ulid, _photo>()

        for await (const data of Silo.findDocs<_photo>(PHOTOS, { $ops: [ { title: { $eq: "All Mighti" } } ] }).collect()) {

            const doc = data as Map<_ulid, _photo>

            for(const [id, photo] of doc) {

                results.set(id, photo)
            }
        }
        
        expect(results.size).toBe(count)
    })

    test("UPDATE ALL", async () => {

        const count = await Silo.patchDocs<_photo>(PHOTOS, { $set: { title: "All Mighter" } })

        const results = new Map<_ulid, _photo>()

        for await (const data of Silo.findDocs<_photo>(PHOTOS, { $ops: [ { title: { $eq: "All Mighter" } } ] }).collect()) {

            const doc = data as Map<_ulid, _photo>

            for(const [id, photo] of doc) {

                results.set(id, photo)
            }
        }

        expect(results.size).toBe(count)
    }, 20000)
})

describe("SQL", async () => {

    test("UPDATE CLAUSE", async () => {

        const count = await Silo.executeSQL<_todo>(`UPDATE ${TODOS} SET title = 'All Mighty' WHERE title LIKE '%est%'`) as number

        const results = await Silo.executeSQL<_todo>(`SELECT * FROM ${TODOS} WHERE title = 'All Mighty'`) as Map<_ulid, _todo>
        
        expect(results.size).toBe(count)
    })

    test("UPDATE ALL", async () => {

        const count = await Silo.executeSQL<_todo>(`UPDATE ${TODOS} SET title = 'All Mightier'`) as number

        const results = await Silo.executeSQL<_todo>(`SELECT * FROM ${TODOS} WHERE title = 'All Mightier'`) as Map<_ulid, _todo>
        
        expect(results.size).toBe(count)
    }, 20000)
})