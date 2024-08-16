import { test, expect, describe } from 'bun:test'
import Silo from '../../src/Stawrij'
import { commentsURL, usersURL } from '../data'
import { mkdirSync, rmSync } from 'node:fs'

rmSync(process.env.DB_DIR!, {recursive:true})
mkdirSync(process.env.DB_DIR!, {recursive:true})

describe("NO-SQL", async () => {

    const COMMENTS = 'comments'

    await Silo.createSchema(COMMENTS)

    await Silo.importBulkData<_comment>(COMMENTS, new URL(commentsURL), 100)

    let results = new Map<_ulid, _comment>()

    for await (const data of Silo.findDocs<_comment>(COMMENTS, { $limit: 1 }).collect()) {

        const doc = data as Map<_ulid, _comment>

        for(const [id, comment] of doc) {

            results.set(id, comment)
        }
    }

    test("DELETE ONE", async () => {

        const id = Array.from(results.keys())[0]

        await Silo.delDoc(COMMENTS, id)

        results = new Map<_ulid, _comment>()

        for await (const data of Silo.findDocs<_comment>(COMMENTS).collect()) {

            const doc = data as Map<_ulid, _comment>

            for(const [id, comment] of doc) {

                results.set(id, comment)
            }
        }

        const idx = Array.from(results.keys()).findIndex(_id => _id === id)

        expect(idx).toEqual(-1)

    })

    test("DELETE CLAUSE", async () => {

        await Silo.delDocs<_comment>(COMMENTS, { $ops: [ { name: { $like: "%et%" } } ] })

        results = new Map<_ulid, _comment>()

        for await (const data of Silo.findDocs<_comment>(COMMENTS, { $ops: [ { name: { $like: "%et%" } } ] }).collect()) {

            const doc = data as Map<_ulid, _comment>

            for(const [id, comment] of doc) {

                results.set(id, comment)
            }
        }

        expect(results.size).toEqual(0)

    })

    test("DELETE ALL", async () => {

        await Silo.delDocs<_comment>(COMMENTS)

        results = new Map<_ulid, _comment>()

        for await (const data of Silo.findDocs<_comment>(COMMENTS).collect()) {

            const doc = data as Map<_ulid, _comment>

            for(const [id, comment] of doc) {

                results.set(id, comment)
            }
        }

        expect(results.size).toBe(0)

    })
})


describe("SQL", async () => {

    const USERS = 'users'

    await Silo.executeSQL<_user>(`CREATE TABLE ${USERS}`)

    await Silo.importBulkData<_user>(USERS, new URL(usersURL), 100)

    let results = await Silo.executeSQL<_user>(`SELECT * FROM users LIMIT 1`) as Map<_ulid, _user>

    test("DELETE CLAUSE", async () => {

        const name = Array.from(results.values())[0].name

        await Silo.executeSQL<_user>(`DELETE FROM users WHERE name = '${name}'`)

        results = await Silo.executeSQL<_user>(`SELECT * FROM users WHERE name = '${name}'`) as Map<_ulid, _user>
        
        const idx = Array.from(results.values()).findIndex(com => com.name === name)

        expect(idx).toBe(-1)
    })

    test("DELETE ALL", async () => {

        await Silo.executeSQL<_user>(`DELETE FROM users`)

        results = await Silo.executeSQL<_user>(`SELECT * FROM users`) as Map<_ulid, _user>

        expect(results.size).toBe(0)
    }, 10000)
})