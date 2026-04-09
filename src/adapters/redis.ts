import { RedisClient } from 'bun'
import { S3 } from './s3'
import type {
    DeadLetterJob,
    QueueStats,
    StreamJobEntry,
    WriteJob,
    WriteJobStatus
} from '../types/write-queue'

export class Redis {
    static readonly WRITE_STREAM = 'fylo:writes'

    static readonly WRITE_GROUP = 'fylo-workers'

    static readonly DEAD_LETTER_STREAM = 'fylo:writes:dead'

    private client: RedisClient

    private static LOGGING = process.env.LOGGING

    constructor() {
        const redisUrl = process.env.REDIS_URL
        if (!redisUrl) throw new Error('REDIS_URL environment variable is required')

        this.client = new RedisClient(redisUrl, {
            connectionTimeout: process.env.REDIS_CONN_TIMEOUT
                ? Number(process.env.REDIS_CONN_TIMEOUT)
                : undefined,
            idleTimeout: process.env.REDIS_IDLE_TIMEOUT
                ? Number(process.env.REDIS_IDLE_TIMEOUT)
                : undefined,
            autoReconnect: process.env.REDIS_AUTO_CONNECT ? true : undefined,
            maxRetries: process.env.REDIS_MAX_RETRIES
                ? Number(process.env.REDIS_MAX_RETRIES)
                : undefined,
            enableOfflineQueue: process.env.REDIS_ENABLE_OFFLINE_QUEUE ? true : undefined,
            enableAutoPipelining: process.env.REDIS_ENABLE_AUTO_PIPELINING ? true : undefined,
            tls: process.env.REDIS_TLS ? true : undefined
        })

        this.client.onconnect = () => {
            if (Redis.LOGGING) console.log('Client Connected')
        }

        this.client.onclose = (err) => console.error('Redis client connection closed', err.message)

        this.client.connect()
    }

    private async ensureWriteGroup() {
        if (!this.client.connected) throw new Error('Redis not connected!')

        try {
            await this.client.send('XGROUP', [
                'CREATE',
                Redis.WRITE_STREAM,
                Redis.WRITE_GROUP,
                '$',
                'MKSTREAM'
            ])
        } catch (err) {
            if (!(err instanceof Error) || !err.message.includes('BUSYGROUP')) throw err
        }
    }

    private static hashKey(jobId: string) {
        return `fylo:job:${jobId}`
    }

    private static docKey(collection: string, docId: _ttid) {
        return `fylo:doc:${collection}:${docId}`
    }

    private static lockKey(collection: string, docId: _ttid) {
        return `fylo:lock:${collection}:${docId}`
    }

    private static parseHash(values: unknown): Record<string, string> {
        if (!Array.isArray(values)) return {}

        const parsed: Record<string, string> = {}

        for (let i = 0; i < values.length; i += 2) {
            parsed[String(values[i])] = String(values[i + 1] ?? '')
        }

        return parsed
    }

    async publish(collection: string, action: 'insert' | 'delete', keyId: string | _ttid) {
        if (this.client.connected) {
            await this.client.publish(
                S3.getBucketFormat(collection),
                JSON.stringify({ action, keyId })
            )
        }
    }

    async claimTTID(_id: _ttid, ttlSeconds: number = 10): Promise<boolean> {
        if (!this.client.connected) return false

        const result = await this.client.send('SET', [
            `ttid:${_id}`,
            '1',
            'NX',
            'EX',
            String(ttlSeconds)
        ])

        return result === 'OK'
    }

    async enqueueWrite<T extends Record<string, any>>(job: WriteJob<T>) {
        if (!this.client.connected) throw new Error('Redis not connected!')

        await this.ensureWriteGroup()

        const now = Date.now()
        const payload = JSON.stringify(job.payload)

        await this.client.send('HSET', [
            Redis.hashKey(job.jobId),
            'jobId',
            job.jobId,
            'collection',
            job.collection,
            'docId',
            job.docId,
            'operation',
            job.operation,
            'payload',
            payload,
            'status',
            job.status,
            'attempts',
            String(job.attempts),
            'createdAt',
            String(job.createdAt),
            'updatedAt',
            String(now),
            'nextAttemptAt',
            String(job.nextAttemptAt ?? now)
        ])

        await this.client.send('HSET', [
            Redis.docKey(job.collection, job.docId),
            'status',
            'queued',
            'lastJobId',
            job.jobId,
            'updatedAt',
            String(now)
        ])

        return await this.client.send('XADD', [
            Redis.WRITE_STREAM,
            '*',
            'jobId',
            job.jobId,
            'collection',
            job.collection,
            'docId',
            job.docId,
            'operation',
            job.operation
        ])
    }

    async readWriteJobs(
        workerId: string,
        count: number = 1,
        blockMs: number = 1000
    ): Promise<Array<StreamJobEntry>> {
        if (!this.client.connected) throw new Error('Redis not connected!')

        await this.ensureWriteGroup()

        const rows = await this.client.send('XREADGROUP', [
            'GROUP',
            Redis.WRITE_GROUP,
            workerId,
            'COUNT',
            String(count),
            'BLOCK',
            String(blockMs),
            'STREAMS',
            Redis.WRITE_STREAM,
            '>'
        ])

        if (!Array.isArray(rows) || rows.length === 0) return []

        const items: Array<StreamJobEntry> = []

        for (const streamRow of rows as unknown[]) {
            if (!Array.isArray(streamRow) || streamRow.length < 2) continue
            const entries = streamRow[1]
            if (!Array.isArray(entries)) continue

            for (const entry of entries as unknown[]) {
                if (!Array.isArray(entry) || entry.length < 2) continue
                const streamId = String(entry[0])
                const fields = Redis.parseHash(entry[1])
                const job = await this.getJob(fields.jobId)
                if (job) items.push({ streamId, job })
            }
        }

        return items
    }

    async ackWriteJob(streamId: string) {
        if (!this.client.connected) throw new Error('Redis not connected!')

        await this.client.send('XACK', [Redis.WRITE_STREAM, Redis.WRITE_GROUP, streamId])
    }

    async deadLetterWriteJob(streamId: string, job: WriteJob, reason?: string) {
        if (!this.client.connected) throw new Error('Redis not connected!')

        const failedAt = Date.now()

        await this.client.send('XADD', [
            Redis.DEAD_LETTER_STREAM,
            '*',
            'jobId',
            job.jobId,
            'collection',
            job.collection,
            'docId',
            job.docId,
            'operation',
            job.operation,
            'reason',
            reason ?? '',
            'failedAt',
            String(failedAt)
        ])

        await this.ackWriteJob(streamId)
    }

    async claimPendingJobs(
        workerId: string,
        minIdleMs: number = 30_000,
        count: number = 10
    ): Promise<Array<StreamJobEntry>> {
        if (!this.client.connected) throw new Error('Redis not connected!')

        await this.ensureWriteGroup()

        const result = await this.client.send('XAUTOCLAIM', [
            Redis.WRITE_STREAM,
            Redis.WRITE_GROUP,
            workerId,
            String(minIdleMs),
            '0-0',
            'COUNT',
            String(count)
        ])

        if (!Array.isArray(result) || result.length < 2 || !Array.isArray(result[1])) return []

        const items: Array<StreamJobEntry> = []

        for (const entry of result[1] as unknown[]) {
            if (!Array.isArray(entry) || entry.length < 2) continue
            const streamId = String(entry[0])
            const fields = Redis.parseHash(entry[1])
            const job = await this.getJob(fields.jobId)
            if (job) items.push({ streamId, job })
        }

        return items
    }

    async setJobStatus(
        jobId: string,
        status: WriteJobStatus,
        extra: Partial<Pick<WriteJob, 'workerId' | 'error' | 'attempts' | 'nextAttemptAt'>> = {}
    ) {
        if (!this.client.connected) throw new Error('Redis not connected!')

        const args = [Redis.hashKey(jobId), 'status', status, 'updatedAt', String(Date.now())]

        if (extra.workerId) args.push('workerId', extra.workerId)
        if (extra.error) args.push('error', extra.error)
        if (typeof extra.attempts === 'number') args.push('attempts', String(extra.attempts))
        if (typeof extra.nextAttemptAt === 'number')
            args.push('nextAttemptAt', String(extra.nextAttemptAt))

        await this.client.send('HSET', args)
    }

    async setDocStatus(collection: string, docId: _ttid, status: WriteJobStatus, jobId?: string) {
        if (!this.client.connected) throw new Error('Redis not connected!')

        const args = [
            Redis.docKey(collection, docId),
            'status',
            status,
            'updatedAt',
            String(Date.now())
        ]

        if (jobId) args.push('lastJobId', jobId)

        await this.client.send('HSET', args)
    }

    async getJob(jobId: string): Promise<WriteJob | null> {
        if (!this.client.connected) throw new Error('Redis not connected!')

        const hash = Redis.parseHash(await this.client.send('HGETALL', [Redis.hashKey(jobId)]))

        if (Object.keys(hash).length === 0) return null

        return {
            jobId: hash.jobId,
            collection: hash.collection,
            docId: hash.docId as _ttid,
            operation: hash.operation as WriteJob['operation'],
            payload: JSON.parse(hash.payload),
            status: hash.status as WriteJobStatus,
            attempts: Number(hash.attempts ?? 0),
            createdAt: Number(hash.createdAt ?? 0),
            updatedAt: Number(hash.updatedAt ?? 0),
            nextAttemptAt: Number(hash.nextAttemptAt ?? 0) || undefined,
            workerId: hash.workerId || undefined,
            error: hash.error || undefined
        }
    }

    async getDocStatus(collection: string, docId: _ttid) {
        if (!this.client.connected) throw new Error('Redis not connected!')

        const hash = Redis.parseHash(
            await this.client.send('HGETALL', [Redis.docKey(collection, docId)])
        )

        return Object.keys(hash).length > 0 ? hash : null
    }

    async readDeadLetters(count: number = 10): Promise<Array<DeadLetterJob>> {
        if (!this.client.connected) throw new Error('Redis not connected!')

        const rows = await this.client.send('XRANGE', [
            Redis.DEAD_LETTER_STREAM,
            '-',
            '+',
            'COUNT',
            String(count)
        ])

        if (!Array.isArray(rows)) return []

        const items: Array<DeadLetterJob> = []

        for (const row of rows as unknown[]) {
            if (!Array.isArray(row) || row.length < 2) continue
            const streamId = String(row[0])
            const fields = Redis.parseHash(row[1])
            const job = await this.getJob(fields.jobId)

            if (job) {
                items.push({
                    streamId,
                    job,
                    reason: fields.reason || undefined,
                    failedAt: Number(fields.failedAt ?? 0)
                })
            }
        }

        return items
    }

    async replayDeadLetter(streamId: string): Promise<WriteJob | null> {
        if (!this.client.connected) throw new Error('Redis not connected!')

        const rows = await this.client.send('XRANGE', [
            Redis.DEAD_LETTER_STREAM,
            streamId,
            streamId,
            'COUNT',
            '1'
        ])

        if (!Array.isArray(rows) || rows.length === 0) return null

        const row = rows[0]
        if (!Array.isArray(row) || row.length < 2) return null

        const fields = Redis.parseHash(row[1])
        const job = await this.getJob(fields.jobId)

        if (!job) return null

        const replayed: WriteJob = {
            ...job,
            status: 'queued',
            error: undefined,
            workerId: undefined,
            attempts: 0,
            updatedAt: Date.now(),
            nextAttemptAt: Date.now()
        }

        await this.enqueueWrite(replayed)
        await this.client.send('XDEL', [Redis.DEAD_LETTER_STREAM, streamId])

        return replayed
    }

    async getQueueStats(): Promise<QueueStats> {
        if (!this.client.connected) throw new Error('Redis not connected!')

        await this.ensureWriteGroup()

        const [queuedRaw, deadRaw, pendingRaw] = await Promise.all([
            this.client.send('XLEN', [Redis.WRITE_STREAM]),
            this.client.send('XLEN', [Redis.DEAD_LETTER_STREAM]),
            this.client.send('XPENDING', [Redis.WRITE_STREAM, Redis.WRITE_GROUP])
        ])

        const pending = Array.isArray(pendingRaw) ? Number(pendingRaw[0] ?? 0) : 0

        return {
            queued: Number(queuedRaw ?? 0),
            pending,
            deadLetters: Number(deadRaw ?? 0)
        }
    }

    async acquireDocLock(collection: string, docId: _ttid, jobId: string, ttlSeconds: number = 60) {
        if (!this.client.connected) throw new Error('Redis not connected!')

        const result = await this.client.send('SET', [
            Redis.lockKey(collection, docId),
            jobId,
            'NX',
            'EX',
            String(ttlSeconds)
        ])

        return result === 'OK'
    }

    async releaseDocLock(collection: string, docId: _ttid, jobId: string) {
        if (!this.client.connected) throw new Error('Redis not connected!')

        const key = Redis.lockKey(collection, docId)
        const current = await this.client.send('GET', [key])
        if (current === jobId) await this.client.send('DEL', [key])
    }

    async *subscribe(collection: string) {
        if (!this.client.connected) throw new Error('Redis not connected!')

        const client = this.client

        const stream = new ReadableStream({
            async start(controller) {
                await client.subscribe(S3.getBucketFormat(collection), (message) => {
                    controller.enqueue(message)
                })
            }
        })

        const reader = stream.getReader()

        while (true) {
            const { done, value } = await reader.read()
            if (done) break
            const parsed = JSON.parse(value)
            if (
                typeof parsed !== 'object' ||
                parsed === null ||
                !('action' in parsed) ||
                !('keyId' in parsed)
            )
                continue
            yield parsed
        }
    }
}
