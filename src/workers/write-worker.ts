import Fylo from "../index"
import { Redis } from "../adapters/redis"
import type { StreamJobEntry, WriteJob } from "../types/write-queue"

export class WriteWorker {

    private static readonly MAX_WRITE_ATTEMPTS = Number(process.env.FYLO_WRITE_MAX_ATTEMPTS ?? 3)

    private static readonly WRITE_RETRY_BASE_MS = Number(process.env.FYLO_WRITE_RETRY_BASE_MS ?? 10)

    private readonly fylo: Fylo

    private readonly redis: Redis

    readonly workerId: string

    constructor(workerId: string = Bun.randomUUIDv7()) {
        this.workerId = workerId
        this.fylo = new Fylo()
        this.redis = new Redis()
    }

    async recoverPending(minIdleMs: number = 30_000, count: number = 10) {
        const jobs = await this.redis.claimPendingJobs(this.workerId, minIdleMs, count)
        for(const job of jobs) await this.processJob(job)
        return jobs.length
    }

    async processNext(count: number = 1, blockMs: number = 1000) {
        const jobs = await this.redis.readWriteJobs(this.workerId, count, blockMs)
        for(const job of jobs) await this.processJob(job)
        return jobs.length
    }

    async processJob({ streamId, job }: StreamJobEntry) {
        if(job.nextAttemptAt && job.nextAttemptAt > Date.now()) return false

        const locked = await this.redis.acquireDocLock(job.collection, job.docId, job.jobId)
        if(!locked) return false

        try {
            await this.redis.setJobStatus(job.jobId, 'processing', {
                workerId: this.workerId,
                attempts: job.attempts + 1
            })
            await this.redis.setDocStatus(job.collection, job.docId, 'processing', job.jobId)

            await this.fylo.executeQueuedWrite(job)

            await this.redis.setJobStatus(job.jobId, 'committed', { workerId: this.workerId })
            await this.redis.setDocStatus(job.collection, job.docId, 'committed', job.jobId)
            await this.redis.ackWriteJob(streamId)

            return true

        } catch(err) {
            const attempts = job.attempts + 1
            const message = err instanceof Error ? err.message : String(err)

            if(attempts >= WriteWorker.MAX_WRITE_ATTEMPTS) {
                await this.redis.setJobStatus(job.jobId, 'dead-letter', {
                    workerId: this.workerId,
                    attempts,
                    error: message
                })
                await this.redis.setDocStatus(job.collection, job.docId, 'dead-letter', job.jobId)
                await this.redis.deadLetterWriteJob(streamId, {
                    ...job,
                    attempts,
                    status: 'dead-letter',
                    workerId: this.workerId,
                    error: message
                }, message)
                return false
            }

            const nextAttemptAt = Date.now() + (WriteWorker.WRITE_RETRY_BASE_MS * Math.max(1, 2 ** (attempts - 1)))

            await this.redis.setJobStatus(job.jobId, 'failed', {
                workerId: this.workerId,
                attempts,
                error: message,
                nextAttemptAt
            })
            await this.redis.setDocStatus(job.collection, job.docId, 'failed', job.jobId)

            return false

        } finally {
            await this.redis.releaseDocLock(job.collection, job.docId, job.jobId)
        }
    }

    async processQueuedInsert(job: WriteJob) {
        return await this.fylo.executeQueuedWrite(job)
    }

    async run({
        batchSize = 1,
        blockMs = 1000,
        recoverOnStart = true,
        recoverIdleMs = 30_000,
        stopWhenIdle = false
    }: {
        batchSize?: number
        blockMs?: number
        recoverOnStart?: boolean
        recoverIdleMs?: number
        stopWhenIdle?: boolean
    } = {}) {

        if(recoverOnStart) await this.recoverPending(recoverIdleMs, batchSize)

        while(true) {
            const processed = await this.processNext(batchSize, blockMs)
            if(stopWhenIdle && processed === 0) break
        }
    }
}
