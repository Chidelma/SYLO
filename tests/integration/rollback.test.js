import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Fylo from '../../src'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'
const POSTS = 'rb-post'
const fylo = new Fylo()
mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))
beforeAll(async () => {
    await Fylo.createCollection(POSTS)
})
afterAll(async () => {
    await Fylo.dropCollection(POSTS)
})
describe('NO-SQL', () => {
    test('INSERT then rollback — document is not retrievable', async () => {
        const _id = await fylo.putData(POSTS, {
            userId: 99,
            id: 9001,
            title: 'Rollback Me',
            body: 'This document should disappear after rollback'
        })
        const before = await Fylo.getDoc(POSTS, _id).once()
        expect(Object.keys(before).length).toBe(1)
        await fylo.rollback()
        const after = await Fylo.getDoc(POSTS, _id).once()
        expect(Object.keys(after).length).toBe(0)
    })
    test('DELETE then rollback — document is restored', async () => {
        const freshFylo = new Fylo()
        const _id = await freshFylo.putData(POSTS, {
            userId: 99,
            id: 9002,
            title: 'Restore Me',
            body: 'This document should reappear after delete rollback'
        })
        const deleteInstance = new Fylo()
        await deleteInstance.delDoc(POSTS, _id)
        const after = await Fylo.getDoc(POSTS, _id).once()
        expect(Object.keys(after).length).toBe(0)
        await deleteInstance.rollback()
        const restored = await Fylo.getDoc(POSTS, _id).once()
        expect(Object.keys(restored).length).toBe(1)
        expect(restored[_id].title).toBe('Restore Me')
    })
    test('batch INSERT then rollback — all documents are removed', async () => {
        const batchFylo = new Fylo()
        const batch = [
            { userId: 98, id: 9003, title: 'Batch A', body: 'body a' },
            { userId: 98, id: 9004, title: 'Batch B', body: 'body b' },
            { userId: 98, id: 9005, title: 'Batch C', body: 'body c' }
        ]
        const ids = await batchFylo.batchPutData(POSTS, batch)
        const beforeResults = await Promise.all(ids.map((id) => Fylo.getDoc(POSTS, id).once()))
        expect(beforeResults.every((r) => Object.keys(r).length === 1)).toBe(true)
        await batchFylo.rollback()
        const afterResults = await Promise.all(ids.map((id) => Fylo.getDoc(POSTS, id).once()))
        expect(afterResults.every((r) => Object.keys(r).length === 0)).toBe(true)
    })
})
