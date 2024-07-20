import { test, expect, describe } from 'bun:test'
import Silo from '../../src/Stawrij'
import { photos, todos } from '../data'
import { mkdirSync, rmSync } from 'node:fs' 

rmSync(process.env.DATA_PREFIX!, {recursive:true})
mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

describe("NO-SQL", async () => {

    const PHOTOS = 'photos'

    await Silo.createSchema(PHOTOS)

    await Silo.bulkDataPut<_photo>(PHOTOS, photos.slice(0, 25))

    test("UPDATE ONE", async () => {

        const ids = await Silo.findDocs<_photo>(PHOTOS, { $limit: 1, $onlyIds: true }).collect() as _uuid[]

        await Silo.patchDoc<_photo>(PHOTOS, new Map([[ids[0], { title: "All Mighty" }]]))

        const results = await Silo.findDocs<_photo>(PHOTOS, { $ops: [{ title: { $eq: "All Mighty" } }]}).collect() as Map<_uuid, _photo>
        
        expect(results.size).toBe(1)
    })

    test("UPDATE CLAUSE", async () => {

        const count = await Silo.patchDocs<_photo>(PHOTOS, { title: "All Mighti", $where: { $ops: [{ title: { $like: "%est%" } }] } })

        const results = await Silo.findDocs<_photo>(PHOTOS, { $ops: [ { title: { $eq: "All Mighti" } } ] }).collect() as Map<_uuid, _photo>
        
        expect(results.size).toBe(count)
    })

    test("UPDATE ALL", async () => {

        const count = await Silo.patchDocs<_photo>(PHOTOS, { title: "All Mighter", $where: {} })

        const results = await Silo.findDocs<_photo>(PHOTOS, { $ops: [ { title: { $eq: "All Mighter" } } ] }).collect() as Map<_uuid, _photo>
        
        expect(results.size).toBe(count)
    })
})

describe("SQL", async () => {

    const TODOS = 'todos'

    await Silo.executeSQL<_todo>(`CREATE TABLE ${TODOS}`)

    for(const todo of todos.slice(0, 25)) {

        const keys = Object.keys(todo)
        const values = Object.values(todo).map(val => JSON.stringify(val))

        await Silo.executeSQL<_todo>(`INSERT INTO ${TODOS} (${keys.join(',')}) VALUES (${values.join('\\')})`)
    }

    test("UPDATE CLAUSE", async () => {

        const count = await Silo.executeSQL<_todo>(`UPDATE ${TODOS} SET title = 'All Mighty' WHERE title LIKE '%est%'`) as number

        const cursor = await Silo.executeSQL<_todo>(`SELECT * FROM ${TODOS} WHERE title = 'All Mighty'`) as _storeCursor<_todo>
        
        const results = await cursor.collect() as Map<_uuid, _todo>
        
        expect(results.size).toBe(count)
    })

    test("UPDATE ALL", async () => {

        const count = await Silo.executeSQL<_todo>(`UPDATE ${TODOS} SET title = 'All Mightier'`) as number

        const cursor = await Silo.executeSQL<_todo>(`SELECT * FROM ${TODOS} WHERE title = 'All Mightier'`) as _storeCursor<_todo>
        
        const results = await cursor.collect() as Map<_uuid, _todo>
        
        expect(results.size).toBe(count)
    })
})