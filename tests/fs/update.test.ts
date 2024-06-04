import { test, expect, describe } from 'bun:test'
import Silo from '../../Stawrij'
import { SILO, _photo, photos, _todo, todos } from '../data'
import { mkdirSync, rmSync } from 'node:fs'

Silo.configureStorages({})

rmSync(process.env.DATA_PREFIX!, {recursive:true})

mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

describe("NO-SQL", async () => {

    const PHOTOS = 'photos'

    for(const photo of photos.slice(0, 25)) await Silo.putDoc(SILO, PHOTOS, photo)

    test("UPDATE ONE", async () => {

        let results = await Silo.findDocs<_photo>(PHOTOS, { }).next(1)

        await Silo.patchDoc<_photo>(SILO, PHOTOS, { _id: results[0]._id, title: "All Mighty" })

        results = await Silo.findDocs<_photo>(PHOTOS, { $ops: [{ title: { $eq: "All Mighty" } }]}).next()

        expect(results.length).toBe(1)
    })

    test("UPDATE CLAUSE", async () => {

        const count = await Silo.patchDocs<_photo>(SILO, PHOTOS, { title: "All Mighti", $where: { $ops: [{ title: { $like: "%est%" } }] } })

        const results = await Silo.findDocs<_photo>(PHOTOS, { $ops: [ { title: { $eq: "All Mighti" } } ] }).next()

        expect(results.length).toBe(count)
    })

    test("UPDATE ALL", async () => {

        const count = await Silo.patchDocs<_photo>(SILO, PHOTOS, { title: "All Mighter", $where: {} })

        expect(count).toBe(25)
    })
})

describe("SQL", async () => {

    const TODOS = 'todos'

    for(const todo of todos.slice(0, 25)) {
        const keys = Object.keys(todo)
        const values = Object.values(todo).map((val: any) => { 
            if(typeof val === 'string') return `'${val}'` 
            else if(typeof val === 'object') return JSON.stringify(val)
            else return val
        })
        await Silo.putDocSQL<_todo>(SILO, `INSERT INTO ${TODOS} (${keys.join(',')}) VALUES (${values.join(',')})`)
    }

    test("UPDATE CLAUSE", async () => {

        const count = await Silo.patchDocsSQL<_todo>(SILO, `UPDATE ${TODOS} SET title = 'All Mighty' WHERE title LIKE '%est%'`)

        const results = await Silo.findDocsSQL<_todo>(`SELECT * FROM ${TODOS} WHERE title = 'All Mighty'`).next()

        expect(results.length).toBe(count)
    })

    test("UPDATE ALL", async () => {

        const count = await Silo.patchDocsSQL<_todo>(SILO, `UPDATE ${TODOS} SET title = 'All Mightier'`)

        const results = await Silo.findDocsSQL<_todo>(`SELECT * FROM ${TODOS} WHERE title = 'All Mightier'`).next()

        expect(results.length).toBe(count)
    })
})