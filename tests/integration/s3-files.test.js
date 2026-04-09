import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import { mkdtemp, rm, stat } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { Database } from 'bun:sqlite'
import Fylo from '../../src'
const root = await mkdtemp(path.join(os.tmpdir(), 'fylo-s3files-'))
const fylo = new Fylo({ engine: 's3-files', s3FilesRoot: root })
const POSTS = 's3files-posts'
const USERS = 's3files-users'
describe('s3-files engine', () => {
    beforeAll(async () => {
        await fylo.createCollection(POSTS)
        await fylo.createCollection(USERS)
    })
    afterAll(async () => {
        await rm(root, { recursive: true, force: true })
    })
    test('put/get/patch/delete works without Redis or S3 adapters', async () => {
        const id = await fylo.putData(POSTS, {
            title: 'Hello',
            tags: ['bun', 'aws'],
            meta: { score: 1 }
        })
        const created = await fylo.getDoc(POSTS, id).once()
        expect(created[id].title).toBe('Hello')
        expect(created[id].tags).toEqual(['bun', 'aws'])
        const nextId = await fylo.patchDoc(POSTS, {
            [id]: {
                title: 'Hello 2',
                meta: { score: 2 }
            }
        })
        const updated = await fylo.getDoc(POSTS, nextId).once()
        expect(updated[nextId].title).toBe('Hello 2')
        expect(updated[nextId].meta.score).toBe(2)
        expect(await fylo.getDoc(POSTS, id).once()).toEqual({})
        await fylo.delDoc(POSTS, nextId)
        expect(await fylo.getDoc(POSTS, nextId).once()).toEqual({})
    })
    test('findDocs listener is backed by the filesystem event journal', async () => {
        const iter = fylo
            .findDocs(POSTS, {
                $ops: [{ title: { $eq: 'Live event' } }]
            })
            [Symbol.asyncIterator]()
        const pending = iter.next()
        await Bun.sleep(100)
        const id = await fylo.putData(POSTS, { title: 'Live event' })
        const { value } = await pending
        expect(value).toEqual({ [id]: { title: 'Live event' } })
        await iter.return?.()
    })
    test('supports long values without path-length issues', async () => {
        const longBody = 'x'.repeat(5000)
        const id = await fylo.putData(POSTS, {
            title: 'Long payload',
            body: longBody
        })
        const result = await fylo.getDoc(POSTS, id).once()
        expect(result[id].body).toBe(longBody)
    })
    test('stores indexes in a single SQLite database instead of per-entry files', async () => {
        const dbStat = await stat(path.join(root, POSTS, '.fylo', 'index.db'))
        expect(dbStat.isFile()).toBe(true)
        await expect(stat(path.join(root, POSTS, '.fylo', 'indexes'))).rejects.toThrow()
    })
    test('uses SQLite index rows to support exact, range, and contains queries', async () => {
        const queryCollection = 's3files-query'
        await fylo.createCollection(queryCollection)

        const bunId = await fylo.putData(queryCollection, {
            title: 'Bun launch',
            tags: ['bun', 'aws'],
            meta: { score: 10 }
        })
        const nodeId = await fylo.putData(queryCollection, {
            title: 'Node launch',
            tags: ['node'],
            meta: { score: 2 }
        })

        let eqResults = {}
        for await (const data of fylo
            .findDocs(queryCollection, {
                $ops: [{ title: { $eq: 'Bun launch' } }]
            })
            .collect()) {
            eqResults = { ...eqResults, ...data }
        }
        expect(Object.keys(eqResults)).toEqual([bunId])

        let rangeResults = {}
        for await (const data of fylo
            .findDocs(queryCollection, {
                $ops: [{ ['meta.score']: { $gte: 5 } }]
            })
            .collect()) {
            rangeResults = { ...rangeResults, ...data }
        }
        expect(Object.keys(rangeResults)).toEqual([bunId])

        let containsResults = {}
        for await (const data of fylo
            .findDocs(queryCollection, {
                $ops: [{ tags: { $contains: 'aws' } }]
            })
            .collect()) {
            containsResults = { ...containsResults, ...data }
        }
        expect(Object.keys(containsResults)).toEqual([bunId])
        expect(containsResults[nodeId]).toBeUndefined()

        const db = new Database(path.join(root, queryCollection, '.fylo', 'index.db'))
        const rows = db
            .query(
                `SELECT doc_id, field_path, raw_value, value_type, numeric_value
                 FROM doc_index_entries
                 WHERE doc_id = ?
                 ORDER BY field_path, raw_value`
            )
            .all(bunId)
        db.close()

        expect(rows).toEqual(
            expect.arrayContaining([
                expect.objectContaining({
                    doc_id: bunId,
                    field_path: 'title',
                    raw_value: 'Bun launch',
                    value_type: 'string',
                    numeric_value: null
                }),
                expect.objectContaining({
                    doc_id: bunId,
                    field_path: 'meta/score',
                    raw_value: '10',
                    value_type: 'number',
                    numeric_value: 10
                }),
                expect.objectContaining({
                    doc_id: bunId,
                    field_path: 'tags/1',
                    raw_value: 'aws',
                    value_type: 'string',
                    numeric_value: null
                })
            ])
        )
    })
    test('joins work in s3-files mode', async () => {
        const userId = await fylo.putData(USERS, { id: 42, name: 'Ada' })
        const postId = await fylo.putData(POSTS, { id: 42, title: 'Shared', content: 'join me' })
        const joined = await fylo.joinDocs({
            $leftCollection: USERS,
            $rightCollection: POSTS,
            $mode: 'inner',
            $on: {
                id: { $eq: 'id' }
            }
        })
        expect(joined[`${userId}, ${postId}`]).toBeDefined()
    })
    test('queue APIs are explicitly unsupported', async () => {
        await expect(fylo.queuePutData(POSTS, { title: 'no queue' })).rejects.toThrow(
            'queuePutData is not supported'
        )
        await expect(fylo.processQueuedWrites(1)).rejects.toThrow(
            'processQueuedWrites is not supported'
        )
    })
    test('rejects collection names that are unsafe for cross-platform filesystems', async () => {
        await expect(fylo.createCollection('bad/name')).rejects.toThrow('Invalid collection name')
        await expect(fylo.createCollection('bad\\name')).rejects.toThrow('Invalid collection name')
        await expect(fylo.createCollection('bad:name')).rejects.toThrow('Invalid collection name')
    })
    test('static helpers can use s3-files through env defaults', async () => {
        const prevEngine = process.env.FYLO_STORAGE_ENGINE
        const prevRoot = process.env.FYLO_S3FILES_ROOT
        process.env.FYLO_STORAGE_ENGINE = 's3-files'
        process.env.FYLO_S3FILES_ROOT = root
        const collection = 's3files-static'
        await Fylo.createCollection(collection)
        const id = await fylo.putData(collection, { title: 'Static path' })
        const result = await Fylo.getDoc(collection, id).once()
        expect(result[id].title).toBe('Static path')
        if (prevEngine === undefined) delete process.env.FYLO_STORAGE_ENGINE
        else process.env.FYLO_STORAGE_ENGINE = prevEngine
        if (prevRoot === undefined) delete process.env.FYLO_S3FILES_ROOT
        else process.env.FYLO_S3FILES_ROOT = prevRoot
    })
})
