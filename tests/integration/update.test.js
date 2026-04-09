import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Fylo from '../../src'
import { photosURL, todosURL } from '../data'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'
const PHOTOS = `photo`
const TODOS = `todo`
const fylo = new Fylo()
mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))
beforeAll(async () => {
    await Promise.all([Fylo.createCollection(PHOTOS), fylo.executeSQL(`CREATE TABLE ${TODOS}`)])
    try {
        await fylo.importBulkData(PHOTOS, new URL(photosURL), 100)
        await fylo.importBulkData(TODOS, new URL(todosURL), 100)
    } catch {
        await fylo.rollback()
    }
})
afterAll(async () => {
    await Promise.all([Fylo.dropCollection(PHOTOS), fylo.executeSQL(`DROP TABLE ${TODOS}`)])
})
describe('NO-SQL', async () => {
    test('UPDATE ONE', async () => {
        const ids = []
        for await (const data of Fylo.findDocs(PHOTOS, { $limit: 1, $onlyIds: true }).collect()) {
            ids.push(data)
        }
        try {
            await fylo.patchDoc(PHOTOS, { [ids.shift()]: { title: 'All Mighty' } })
        } catch {
            await fylo.rollback()
        }
        let results = {}
        for await (const data of Fylo.findDocs(PHOTOS, {
            $ops: [{ title: { $eq: 'All Mighty' } }]
        }).collect()) {
            results = { ...results, ...data }
        }
        expect(Object.keys(results).length).toBe(1)
    })
    test('UPDATE CLAUSE', async () => {
        let count = -1
        try {
            count = await fylo.patchDocs(PHOTOS, {
                $set: { title: 'All Mighti' },
                $where: { $ops: [{ title: { $like: '%est%' } }] }
            })
        } catch {
            await fylo.rollback()
        }
        let results = {}
        for await (const data of Fylo.findDocs(PHOTOS, {
            $ops: [{ title: { $eq: 'All Mighti' } }]
        }).collect()) {
            results = { ...results, ...data }
        }
        expect(Object.keys(results).length).toBe(count)
    })
    test('UPDATE ALL', async () => {
        let count = -1
        try {
            count = await fylo.patchDocs(PHOTOS, { $set: { title: 'All Mighter' } })
        } catch {
            await fylo.rollback()
        }
        let results = {}
        for await (const data of Fylo.findDocs(PHOTOS, {
            $ops: [{ title: { $eq: 'All Mighter' } }]
        }).collect()) {
            results = { ...results, ...data }
        }
        expect(Object.keys(results).length).toBe(count)
    }, 20000)
})
describe('SQL', async () => {
    test('UPDATE CLAUSE', async () => {
        let count = -1
        try {
            count = await fylo.executeSQL(
                `UPDATE ${TODOS} SET title = 'All Mighty' WHERE title LIKE '%est%'`
            )
        } catch {
            await fylo.rollback()
        }
        const results = await fylo.executeSQL(`SELECT * FROM ${TODOS} WHERE title = 'All Mighty'`)
        expect(Object.keys(results).length).toBe(count)
    })
    test('UPDATE ALL', async () => {
        let count = -1
        try {
            count = await fylo.executeSQL(`UPDATE ${TODOS} SET title = 'All Mightier'`)
        } catch {
            await fylo.rollback()
        }
        const results = await fylo.executeSQL(`SELECT * FROM ${TODOS} WHERE title = 'All Mightier'`)
        expect(Object.keys(results).length).toBe(count)
    }, 20000)
})
