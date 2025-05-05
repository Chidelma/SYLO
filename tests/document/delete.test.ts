import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import Silo from '../../src/Stawrij'
import { commentsURL, usersURL } from '../data'
import { mkdir, rm, exists } from 'node:fs/promises'

const COMMENTS = `comments`
const USERS = `users`

let commentsResults = new Map<_ulid, _comment>()
let usersResults = new Map<_ulid, _user>()

beforeAll(async () => {
    if(await exists(process.env.DB_DIR!)) {
        await rm(process.env.DB_DIR!, {recursive:true})
    }

    await mkdir(process.env.DB_DIR!, {recursive:true})
    
    await Promise.all([
        Silo.createCollection(COMMENTS), 
        Silo.executeSQL<_post>(`CREATE TABLE ${USERS}`)
    ])
    
    await Promise.all([
        Silo.importBulkData<_comment>(COMMENTS, new URL(commentsURL), 100),
        Silo.importBulkData<_user>(USERS, new URL(usersURL), 100)
    ])

    for await (const data of Silo.findDocs<_comment>(COMMENTS, { $limit: 1 }).collect()) {

        const doc = data as Map<_ulid, _comment>

        for(const [id, comment] of doc) {

            commentsResults.set(id, comment)
        }
    }

    usersResults = await Silo.executeSQL<_user>(`SELECT * FROM ${USERS} LIMIT 1`) as Map<_ulid, _user>
})

afterAll(async () => {
    await Promise.all([Silo.dropCollection(COMMENTS), Silo.executeSQL<_user>(`DROP TABLE ${USERS}`)])
    await rm(process.env.DB_DIR!, {recursive:true})
})

describe("NO-SQL", async () => {

    test("DELETE ONE", async () => {

        const id = Array.from(commentsResults.keys())[0]

        await Silo.delDoc(COMMENTS, id)

        commentsResults = new Map<_ulid, _comment>()

        for await (const data of Silo.findDocs<_comment>(COMMENTS).collect()) {

            const doc = data as Map<_ulid, _comment>

            for(const [id, comment] of doc) {

                commentsResults.set(id, comment)
            }
        }

        const idx = Array.from(commentsResults.keys()).findIndex(_id => _id === id)

        expect(idx).toEqual(-1)

    })

    test("DELETE CLAUSE", async () => {

        await Silo.delDocs<_comment>(COMMENTS, { $ops: [ { name: { $like: "%et%" } } ] })

        commentsResults = new Map<_ulid, _comment>()

        for await (const data of Silo.findDocs<_comment>(COMMENTS, { $ops: [ { name: { $like: "%et%" } } ] }).collect()) {

            const doc = data as Map<_ulid, _comment>

            for(const [id, comment] of doc) {

                commentsResults.set(id, comment)
            }
        }

        expect(commentsResults.size).toEqual(0)

    })

    test("DELETE ALL", async () => {

        await Silo.delDocs<_comment>(COMMENTS)

        commentsResults = new Map<_ulid, _comment>()

        for await (const data of Silo.findDocs<_comment>(COMMENTS).collect()) {

            const doc = data as Map<_ulid, _comment>

            for(const [id, comment] of doc) {

                commentsResults.set(id, comment)
            }
        }

        expect(commentsResults.size).toBe(0)

    })
})


describe("SQL", async () => {

    test("DELETE CLAUSE", async () => {

        const name = Array.from(usersResults.values())[0].name

        await Silo.executeSQL<_user>(`DELETE FROM ${USERS} WHERE name = '${name}'`)

        usersResults = await Silo.executeSQL<_user>(`SELECT * FROM ${USERS} WHERE name = '${name}'`) as Map<_ulid, _user>
        
        const idx = Array.from(usersResults.values()).findIndex(com => com.name === name)

        expect(idx).toBe(-1)
    })

    test("DELETE ALL", async () => {

        await Silo.executeSQL<_user>(`DELETE FROM ${USERS}`)

        usersResults = await Silo.executeSQL<_user>(`SELECT * FROM ${USERS}`) as Map<_ulid, _user>

        expect(usersResults.size).toBe(0)
    })
})