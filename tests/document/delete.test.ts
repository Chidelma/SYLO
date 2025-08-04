import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Sylo from '../../src'
import { commentsURL, usersURL } from '../data'

const COMMENTS = `comment`
const USERS = `user`

let commentsResults: Record<_ttid, _comment> = {}
let usersResults: Record<_ttid, _user> = {}

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

    await Promise.all([
        Sylo.createCollection(COMMENTS), 
        sylo.executeSQL<_post>(`CREATE TABLE ${USERS}`)
    ])

    try {
        
        await Promise.all([
            sylo.importBulkData<_comment>(COMMENTS, new URL(commentsURL), 100),
            sylo.importBulkData<_user>(USERS, new URL(usersURL), 100)
        ])

    } catch {
        await sylo.rollback()
    }

    for await (const data of Sylo.findDocs<_comment>(COMMENTS, { $limit: 1 }).collect()) {

        commentsResults = { ...commentsResults, ...data as Record<_ttid, _comment> }
        
    }

    usersResults = await sylo.executeSQL<_user>(`SELECT * FROM ${USERS} LIMIT 1`) as Record<_ttid, _user>
})

afterAll(async () => {
    await Promise.all([Sylo.dropCollection(COMMENTS), sylo.executeSQL<_user>(`DROP TABLE ${USERS}`)])
})

describe("NO-SQL", async () => {

    test("DELETE ONE", async () => {

        const id = Object.keys(commentsResults).shift()!

        try {
            await sylo.delDoc(COMMENTS, id)
        } catch {
            await sylo.rollback()
        }

        commentsResults = {}

        for await (const data of Sylo.findDocs<_comment>(COMMENTS).collect()) {

            commentsResults = { ...commentsResults, ...data as Record<_ttid, _comment> }
        }

        const idx = Object.keys(commentsResults).findIndex(_id => _id === id)

        expect(idx).toEqual(-1)

    })

    test("DELETE CLAUSE", async () => {

        try {
            await sylo.delDocs<_comment>(COMMENTS, { $ops: [ { name: { $like: "%et%" } } ] })
            //console.log
        } catch(e) {
            console.error(e)
            await sylo.rollback()
        }
        
        commentsResults = {}

        for await (const data of Sylo.findDocs<_comment>(COMMENTS, { $ops: [ { name: { $like: "%et%" } } ] }).collect()) {

            // console.log(data)
            
            commentsResults = { ...commentsResults, ...data as Record<_ttid, _comment> }
        }

        expect(Object.keys(commentsResults).length).toEqual(0)
    })

    test("DELETE ALL", async () => {

        try {
            await sylo.delDocs<_comment>(COMMENTS)
        } catch {
            await sylo.rollback()
        }

        commentsResults = {}

        for await (const data of Sylo.findDocs<_comment>(COMMENTS).collect()) {

            commentsResults = { ...commentsResults, ...data as Record<_ttid, _comment> }
        }

        expect(Object.keys(commentsResults).length).toEqual(0)
    })
})


describe("SQL", async () => {

    test("DELETE CLAUSE", async () => {

        const name = Object.values(usersResults).shift()!.name

        try {
            await sylo.executeSQL<_user>(`DELETE FROM ${USERS} WHERE name = '${name}'`)
        } catch {
            await sylo.rollback()
        } 

        usersResults = await sylo.executeSQL<_user>(`SELECT * FROM ${USERS} WHERE name = '${name}'`) as Record<_ttid, _user>
        
        const idx = Object.values(usersResults).findIndex(com => com.name === name)

        expect(idx).toBe(-1)
    })

    test("DELETE ALL", async () => {

        try {
            await sylo.executeSQL<_user>(`DELETE FROM ${USERS}`)
        } catch {
            await sylo.rollback()
        }

        usersResults = await sylo.executeSQL<_user>(`SELECT * FROM ${USERS}`) as Record<_ttid, _user>

        expect(Object.keys(usersResults).length).toBe(0)
    })
})