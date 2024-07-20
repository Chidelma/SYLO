import { test, expect, describe } from 'bun:test'
import Silo from '../../src/Stawrij'
import { comments, users } from '../data'
import { mkdirSync, rmSync } from 'node:fs'

rmSync(process.env.DATA_PREFIX!, {recursive:true})
mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

describe("NO-SQL", async () => {

    const COMMENTS = 'comments'

    await Silo.createSchema(COMMENTS)

    await Silo.bulkDataPut<_comment>(COMMENTS, comments.slice(0, 25))

    let results = await Silo.findDocs<_comment>(COMMENTS, { $limit: 1 }).collect() as Map<_uuid, _comment>

    test("DELETE ONE", async () => {

        const id = Array.from(results.keys())[0]

        await Silo.delDoc(COMMENTS, id)

        results = await Silo.findDocs<_comment>(COMMENTS).collect() as Map<_uuid, _comment>

        const idx = Array.from(results.keys()).findIndex(_id => _id === id)

        expect(idx).toEqual(-1)

    })

    test("DELETE CLAUSE", async () => {

        await Silo.delDocs<_comment>(COMMENTS, { $ops: [ { name: { $like: "%et%" } } ] })

        results = await Silo.findDocs<_comment>(COMMENTS, { $ops: [ { name: { $like: "%et%" } } ] }).collect() as Map<_uuid, _comment>

        expect(results.size).toEqual(0)

    })

    test("DELETE ALL", async () => {

        await Silo.delDocs<_comment>(COMMENTS)

        results = await Silo.findDocs<_comment>(COMMENTS).collect() as Map<_uuid, _comment>

        expect(results.size).toBe(0)

    })
})


describe("SQL", async () => {

    const USERS = 'users'

    await Silo.executeSQL<_user>(`CREATE TABLE ${USERS}`)

    for(const user of users.slice(0, 25)) {

        const keys = Object.keys(user)
        const values = Object.values(user).map(val => JSON.stringify(val))

        await Silo.executeSQL<_user>(`INSERT INTO ${USERS} (${keys.join(',')}) VALUES (${values.join('\\')})`)
    }

    let cursor = await Silo.executeSQL<_user>(`SELECT * FROM users LIMIT 1`) as _storeCursor<_user>

    let results = await cursor.collect() as Map<_uuid, _user>

    test("DELETE CLAUSE", async () => {

        const name = Array.from(results.values())[0].name

        await Silo.executeSQL<_user>(`DELETE FROM users WHERE name = '${name}'`)

        cursor = await Silo.executeSQL<_user>(`SELECT * FROM users WHERE name = '${name}'`) as _storeCursor<_user>

        results = await cursor.collect() as Map<_uuid, _user>
        
        const idx = Array.from(results.values()).findIndex(com => com.name === name)

        expect(idx).toBe(-1)
    })

    test("DELETE ALL", async () => {

        await Silo.executeSQL<_user>(`DELETE FROM users`)

        cursor = await Silo.executeSQL<_user>(`SELECT * FROM users`) as _storeCursor<_user>

        results = await cursor.collect() as Map<_uuid, _user>

        expect(results.size).toBe(0)
    })
})