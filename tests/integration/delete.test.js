import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Fylo from '../../src'
import { commentsURL, usersURL } from '../data'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'
const COMMENTS = `comment`
const USERS = `user`
let commentsResults = {}
let usersResults = {}
const fylo = new Fylo()
mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))
beforeAll(async () => {
    await Promise.all([Fylo.createCollection(COMMENTS), fylo.executeSQL(`CREATE TABLE ${USERS}`)])
    try {
        await Promise.all([
            fylo.importBulkData(COMMENTS, new URL(commentsURL), 100),
            fylo.importBulkData(USERS, new URL(usersURL), 100)
        ])
    } catch {
        await fylo.rollback()
    }
    for await (const data of Fylo.findDocs(COMMENTS, { $limit: 1 }).collect()) {
        commentsResults = { ...commentsResults, ...data }
    }
    usersResults = await fylo.executeSQL(`SELECT * FROM ${USERS} LIMIT 1`)
})
afterAll(async () => {
    await Promise.all([Fylo.dropCollection(COMMENTS), fylo.executeSQL(`DROP TABLE ${USERS}`)])
})
describe('NO-SQL', async () => {
    test('DELETE ONE', async () => {
        const id = Object.keys(commentsResults).shift()
        try {
            await fylo.delDoc(COMMENTS, id)
        } catch {
            await fylo.rollback()
        }
        commentsResults = {}
        for await (const data of Fylo.findDocs(COMMENTS).collect()) {
            commentsResults = { ...commentsResults, ...data }
        }
        const idx = Object.keys(commentsResults).findIndex((_id) => _id === id)
        expect(idx).toEqual(-1)
    })
    test('DELETE CLAUSE', async () => {
        try {
            await fylo.delDocs(COMMENTS, { $ops: [{ name: { $like: '%et%' } }] })
        } catch (e) {
            console.error(e)
            await fylo.rollback()
        }
        commentsResults = {}
        for await (const data of Fylo.findDocs(COMMENTS, {
            $ops: [{ name: { $like: '%et%' } }]
        }).collect()) {
            commentsResults = { ...commentsResults, ...data }
        }
        expect(Object.keys(commentsResults).length).toEqual(0)
    })
    test('DELETE ALL', async () => {
        try {
            await fylo.delDocs(COMMENTS)
        } catch {
            await fylo.rollback()
        }
        commentsResults = {}
        for await (const data of Fylo.findDocs(COMMENTS).collect()) {
            commentsResults = { ...commentsResults, ...data }
        }
        expect(Object.keys(commentsResults).length).toEqual(0)
    })
})
describe('SQL', async () => {
    test('DELETE CLAUSE', async () => {
        const name = Object.values(usersResults).shift().name
        try {
            await fylo.executeSQL(`DELETE FROM ${USERS} WHERE name = '${name}'`)
        } catch {
            await fylo.rollback()
        }
        usersResults = await fylo.executeSQL(`SELECT * FROM ${USERS} WHERE name = '${name}'`)
        const idx = Object.values(usersResults).findIndex((com) => com.name === name)
        expect(idx).toBe(-1)
    })
    test('DELETE ALL', async () => {
        try {
            await fylo.executeSQL(`DELETE FROM ${USERS}`)
        } catch {
            await fylo.rollback()
        }
        usersResults = await fylo.executeSQL(`SELECT * FROM ${USERS}`)
        expect(Object.keys(usersResults).length).toBe(0)
    })
})
