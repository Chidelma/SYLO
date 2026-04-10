import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import Fylo from '../../src'
const root = await mkdtemp(path.join(os.tmpdir(), 'fylo-s3files-'))
const fylo = new Fylo({ root })
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
    test('stores only user document data in the file body', async () => {
        const id = await fylo.putData(POSTS, {
            title: 'Lean doc',
            body: 'payload only'
        })
        const raw = JSON.parse(
            await readFile(path.join(root, POSTS, '.fylo', 'docs', id.slice(0, 2), `${id}.json`), 'utf8')
        )

        expect(raw).toEqual({
            title: 'Lean doc',
            body: 'payload only'
        })
        expect(raw.id).toBeUndefined()
        expect(raw.createdAt).toBeUndefined()
        expect(raw.updatedAt).toBeUndefined()
    })
    test('stores indexes in a single collection file instead of SQLite or per-entry files', async () => {
        const indexStat = await stat(
            path.join(root, POSTS, '.fylo', 'indexes', `${POSTS}.idx.json`)
        )
        expect(indexStat.isFile()).toBe(true)
        await expect(stat(path.join(root, POSTS, '.fylo', 'index.db'))).rejects.toThrow()
    })
    test('uses the collection index file to support exact, range, and contains queries', async () => {
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

        const index = JSON.parse(
            await readFile(
                path.join(root, queryCollection, '.fylo', 'indexes', `${queryCollection}.idx.json`),
                'utf8'
            )
        )
        const rows = index.docs[bunId]

        expect(rows).toEqual(
            expect.arrayContaining([
                expect.objectContaining({
                    fieldPath: 'title',
                    rawValue: 'Bun launch',
                    valueType: 'string',
                    numericValue: null
                }),
                expect.objectContaining({
                    fieldPath: 'meta/score',
                    rawValue: '10',
                    valueType: 'number',
                    numericValue: 10
                }),
                expect.objectContaining({
                    fieldPath: 'tags/1',
                    rawValue: 'aws',
                    valueType: 'string',
                    numericValue: null
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
            'queuePutData was removed'
        )
        await expect(fylo.processQueuedWrites(1)).rejects.toThrow('processQueuedWrites was removed')
    })
    test('rejects collection names that are unsafe for cross-platform filesystems', async () => {
        await expect(fylo.createCollection('bad/name')).rejects.toThrow('Invalid collection name')
        await expect(fylo.createCollection('bad\\name')).rejects.toThrow('Invalid collection name')
        await expect(fylo.createCollection('bad:name')).rejects.toThrow('Invalid collection name')
    })
    test('static helpers can use filesystem root env defaults', async () => {
        const prevFyloRoot = process.env.FYLO_ROOT
        const prevS3FilesRoot = process.env.FYLO_S3FILES_ROOT
        process.env.FYLO_ROOT = root
        process.env.FYLO_S3FILES_ROOT = root
        const collection = 's3files-static'
        await Fylo.createCollection(collection)
        const id = await fylo.putData(collection, { title: 'Static path' })
        const result = await Fylo.getDoc(collection, id).once()
        expect(result[id].title).toBe('Static path')
        if (prevFyloRoot === undefined) delete process.env.FYLO_ROOT
        else process.env.FYLO_ROOT = prevFyloRoot
        if (prevS3FilesRoot === undefined) delete process.env.FYLO_S3FILES_ROOT
        else process.env.FYLO_S3FILES_ROOT = prevS3FilesRoot
    })
})
