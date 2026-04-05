import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Fylo from '../../src'
import { commentsURL, usersURL } from '../data'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'

const COMMENTS = `comment`
const USERS = `user`

let commentsResults: Record<_ttid, _comment> = {}
let usersResults: Record<_ttid, _user> = {}

const fylo = new Fylo()

mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))

beforeAll(async () => {

    await Promise.all([
        Fylo.createCollection(COMMENTS), 
        fylo.executeSQL<_post>(`CREATE TABLE ${USERS}`)
    ])

    try {
        
        await Promise.all([
            fylo.importBulkData<_comment>(COMMENTS, new URL(commentsURL), 100),
            fylo.importBulkData<_user>(USERS, new URL(usersURL), 100)
        ])

    } catch {
        await fylo.rollback()
    }

    for await (const data of Fylo.findDocs<_comment>(COMMENTS, { $limit: 1 }).collect()) {

        commentsResults = { ...commentsResults, ...data as Record<_ttid, _comment> }
        
    }

    usersResults = await fylo.executeSQL<_user>(`SELECT * FROM ${USERS} LIMIT 1`) as Record<_ttid, _user>
})

afterAll(async () => {
    await Promise.all([Fylo.dropCollection(COMMENTS), fylo.executeSQL<_user>(`DROP TABLE ${USERS}`)])
})

describe("NO-SQL", async () => {

    test("DELETE ONE", async () => {

        const id = Object.keys(commentsResults).shift()!

        try {
            await fylo.delDoc(COMMENTS, id)
        } catch {
            await fylo.rollback()
        }

        commentsResults = {}

        for await (const data of Fylo.findDocs<_comment>(COMMENTS).collect()) {

            commentsResults = { ...commentsResults, ...data as Record<_ttid, _comment> }
        }

        const idx = Object.keys(commentsResults).findIndex(_id => _id === id)

        expect(idx).toEqual(-1)

    })

    test("DELETE CLAUSE", async () => {

        try {
            await fylo.delDocs<_comment>(COMMENTS, { $ops: [ { name: { $like: "%et%" } } ] })
            //console.log
        } catch(e) {
            console.error(e)
            await fylo.rollback()
        }
        
        commentsResults = {}

        for await (const data of Fylo.findDocs<_comment>(COMMENTS, { $ops: [ { name: { $like: "%et%" } } ] }).collect()) {

            // console.log(data)
            
            commentsResults = { ...commentsResults, ...data as Record<_ttid, _comment> }
        }

        expect(Object.keys(commentsResults).length).toEqual(0)
    })

    test("DELETE ALL", async () => {

        try {
            await fylo.delDocs<_comment>(COMMENTS)
        } catch {
            await fylo.rollback()
        }

        commentsResults = {}

        for await (const data of Fylo.findDocs<_comment>(COMMENTS).collect()) {

            commentsResults = { ...commentsResults, ...data as Record<_ttid, _comment> }
        }

        expect(Object.keys(commentsResults).length).toEqual(0)
    })
})


describe("SQL", async () => {

    test("DELETE CLAUSE", async () => {

        const name = Object.values(usersResults).shift()!.name

        try {
            await fylo.executeSQL<_user>(`DELETE FROM ${USERS} WHERE name = '${name}'`)
        } catch {
            await fylo.rollback()
        } 

        usersResults = await fylo.executeSQL<_user>(`SELECT * FROM ${USERS} WHERE name = '${name}'`) as Record<_ttid, _user>
        
        const idx = Object.values(usersResults).findIndex(com => com.name === name)

        expect(idx).toBe(-1)
    })

    test("DELETE ALL", async () => {

        try {
            await fylo.executeSQL<_user>(`DELETE FROM ${USERS}`)
        } catch {
            await fylo.rollback()
        }

        usersResults = await fylo.executeSQL<_user>(`SELECT * FROM ${USERS}`) as Record<_ttid, _user>

        expect(Object.keys(usersResults).length).toBe(0)
    })
})