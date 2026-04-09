import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Fylo from '../../src'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'
const POSTS = 'queued-post'
mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))
const fylo = new Fylo()
beforeAll(async () => {
    await Fylo.createCollection(POSTS)
})
afterAll(async () => {
    await Fylo.dropCollection(POSTS)
})
describe('queue writes', () => {
    test('queuePutData enqueues and worker commits insert', async () => {
        const queued = await fylo.queuePutData(POSTS, {
            title: 'Queued Title',
            body: 'Queued Body'
        })
        const queuedStatus = await fylo.getJobStatus(queued.jobId)
        expect(queuedStatus?.status).toBe('queued')
        const processed = await fylo.processQueuedWrites(1)
        expect(processed).toBe(1)
        const doc = await Fylo.getDoc(POSTS, queued.docId, false).once()
        expect(Object.keys(doc).length).toBe(1)
    })
    test('putData can return immediately when wait is false', async () => {
        const queued = await fylo.putData(
            POSTS,
            {
                title: 'Async Title',
                body: 'Async Body'
            },
            { wait: false }
        )
        expect(typeof queued).toBe('object')
        expect('jobId' in queued).toBe(true)
        const before = await Fylo.getDoc(POSTS, queued.docId, false).once()
        expect(Object.keys(before).length).toBe(0)
        const processed = await fylo.processQueuedWrites(1)
        expect(processed).toBe(1)
        const stats = await fylo.getQueueStats()
        expect(stats.deadLetters).toBe(0)
        const after = await Fylo.getDoc(POSTS, queued.docId, false).once()
        expect(Object.keys(after).length).toBe(1)
    })
    test('failed jobs can be recovered and eventually dead-lettered', async () => {
        const originalExecute = fylo.executeQueuedWrite.bind(fylo)
        fylo.executeQueuedWrite = async () => {
            throw new Error('simulated write failure')
        }
        try {
            const queued = await fylo.putData(
                POSTS,
                {
                    title: 'Broken Title',
                    body: 'Broken Body'
                },
                { wait: false }
            )
            expect(await fylo.processQueuedWrites(1)).toBe(0)
            expect((await fylo.getJobStatus(queued.jobId))?.status).toBe('failed')
            await Bun.sleep(15)
            expect(await fylo.processQueuedWrites(1, true)).toBe(0)
            expect((await fylo.getJobStatus(queued.jobId))?.status).toBe('failed')
            await Bun.sleep(25)
            expect(await fylo.processQueuedWrites(1, true)).toBe(0)
            expect((await fylo.getJobStatus(queued.jobId))?.status).toBe('dead-letter')
            const deadLetters = await fylo.getDeadLetters()
            expect((await fylo.getQueueStats()).deadLetters).toBeGreaterThan(0)
            expect(deadLetters.some((item) => item.job.jobId === queued.jobId)).toBe(true)
            fylo.executeQueuedWrite = originalExecute
            const replayed = await fylo.replayDeadLetter(deadLetters[0].streamId)
            expect(replayed?.jobId).toBe(queued.jobId)
            expect((await fylo.getQueueStats()).deadLetters).toBe(0)
            expect(await fylo.processQueuedWrites(1)).toBe(1)
            expect((await fylo.getJobStatus(queued.jobId))?.status).toBe('committed')
        } finally {
            fylo.executeQueuedWrite = originalExecute
        }
    })
})
