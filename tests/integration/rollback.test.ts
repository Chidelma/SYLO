import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Sylo from '../../src'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'

/**
 * Rollback mechanism:
 *   - putKeys() pushes S3.delete ops into the transactions stack
 *   - deleteKeys() pushes S3.put ops (restore) into the stack
 *   - executeRollback() pops and executes them in reverse order
 *
 * After rollback, the written data should no longer be retrievable.
 * After a delete rollback, the deleted data should be restored.
 */

const POSTS = 'rb-post'

const sylo = new Sylo()

mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))

beforeAll(async () => {
    await Sylo.createCollection(POSTS)
})

afterAll(async () => {
    await Sylo.dropCollection(POSTS)
})

describe("NO-SQL", () => {

    test("INSERT then rollback — document is not retrievable", async () => {

        const _id = await sylo.putData<_post>(POSTS, {
            userId: 99,
            id: 9001,
            title: 'Rollback Me',
            body: 'This document should disappear after rollback'
        })

        // Verify the document was actually written
        const before = await Sylo.getDoc<_post>(POSTS, _id).once()
        expect(Object.keys(before).length).toBe(1)

        // Rollback undoes all writes made on this Sylo instance
        await sylo.rollback()

        const after = await Sylo.getDoc<_post>(POSTS, _id).once()
        expect(Object.keys(after).length).toBe(0)
    })

    test("DELETE then rollback — document is restored", async () => {

        // Use a fresh Sylo instance so the transactions stack is clean
        const freshSylo = new Sylo()

        const _id = await freshSylo.putData<_post>(POSTS, {
            userId: 99,
            id: 9002,
            title: 'Restore Me',
            body: 'This document should reappear after delete rollback'
        })

        // Clear the insert transactions so rollback only covers the delete
        // (call rollback would delete the just-written doc, defeating the purpose)
        // Instead we use a second instance that has a clean transaction stack
        const deleteInstance = new Sylo()

        await deleteInstance.delDoc(POSTS, _id)

        // Confirm it's gone
        const after = await Sylo.getDoc<_post>(POSTS, _id).once()
        expect(Object.keys(after).length).toBe(0)

        // Rollback the delete — should restore the document
        await deleteInstance.rollback()

        const restored = await Sylo.getDoc<_post>(POSTS, _id).once()
        expect(Object.keys(restored).length).toBe(1)
        expect(restored[_id].title).toBe('Restore Me')
    })

    test("batch INSERT then rollback — all documents are removed", async () => {

        const batchSylo = new Sylo()

        const batch: _post[] = [
            { userId: 98, id: 9003, title: 'Batch A', body: 'body a' },
            { userId: 98, id: 9004, title: 'Batch B', body: 'body b' },
            { userId: 98, id: 9005, title: 'Batch C', body: 'body c' }
        ]

        const ids = await batchSylo.batchPutData<_post>(POSTS, batch)

        // Confirm all written
        const beforeResults = await Promise.all(ids.map(id => Sylo.getDoc<_post>(POSTS, id).once()))
        expect(beforeResults.every(r => Object.keys(r).length === 1)).toBe(true)

        await batchSylo.rollback()

        const afterResults = await Promise.all(ids.map(id => Sylo.getDoc<_post>(POSTS, id).once()))
        expect(afterResults.every(r => Object.keys(r).length === 0)).toBe(true)
    })
})
