/**
 * No-op Redis mock. All methods are silent no-ops so tests never need a
 * running Redis instance. subscribe yields nothing so listener code paths
 * simply exit immediately.
 */
export default class RedisMock {

    private static stream: Array<{
        streamId: string
        jobId: string
        collection: string
        docId: _ttid
        operation: string
        claimedBy?: string
    }> = []

    private static jobs = new Map<string, Record<string, any>>()

    private static docs = new Map<string, Record<string, string>>()

    private static locks = new Map<string, string>()

    private static deadLetters: Array<{ streamId: string, jobId: string, reason?: string, failedAt: number }> = []

    private static nextId = 0

    async publish(_collection: string, _action: 'insert' | 'delete', _keyId: string | _ttid): Promise<void> {}

    async claimTTID(_id: _ttid, _ttlSeconds: number = 10): Promise<boolean> { return true }

    async enqueueWrite(job: Record<string, any>) {
        RedisMock.jobs.set(job.jobId, { ...job, nextAttemptAt: job.nextAttemptAt ?? Date.now() })
        RedisMock.docs.set(`fylo:doc:${job.collection}:${job.docId}`, {
            status: 'queued',
            lastJobId: job.jobId,
            updatedAt: String(Date.now())
        })
        const streamId = String(++RedisMock.nextId)
        RedisMock.stream.push({
            streamId,
            jobId: job.jobId,
            collection: job.collection,
            docId: job.docId,
            operation: job.operation
        })
        return streamId
    }

    async readWriteJobs(workerId: string, count: number = 1): Promise<Array<{ streamId: string, job: Record<string, any> }>> {
        const available = RedisMock.stream
            .filter(entry => !entry.claimedBy)
            .slice(0, count)

        for(const entry of available) entry.claimedBy = workerId

        return available.map(entry => ({
            streamId: entry.streamId,
            job: { ...RedisMock.jobs.get(entry.jobId)! }
        }))
    }

    async ackWriteJob(streamId: string) {
        RedisMock.stream = RedisMock.stream.filter(item => item.streamId !== streamId)
    }

    async deadLetterWriteJob(streamId: string, job: Record<string, any>, reason?: string) {
        RedisMock.deadLetters.push({
            streamId: String(RedisMock.deadLetters.length + 1),
            jobId: job.jobId,
            reason,
            failedAt: Date.now()
        })

        await this.ackWriteJob(streamId)
    }

    async claimPendingJobs(workerId: string, _minIdleMs: number = 30_000, count: number = 10) {
        const pending = RedisMock.stream
            .filter(entry => entry.claimedBy)
            .slice(0, count)

        for(const entry of pending) entry.claimedBy = workerId

        return pending.map(entry => ({
            streamId: entry.streamId,
            job: { ...RedisMock.jobs.get(entry.jobId)! }
        }))
    }

    async setJobStatus(jobId: string, status: string, extra: Record<string, any> = {}) {
        const job = RedisMock.jobs.get(jobId)
        if(job) Object.assign(job, extra, { status, updatedAt: Date.now() })
    }

    async setDocStatus(collection: string, docId: _ttid, status: string, jobId?: string) {
        const key = `fylo:doc:${collection}:${docId}`
        const curr = RedisMock.docs.get(key) ?? {}
        RedisMock.docs.set(key, {
            ...curr,
            status,
            updatedAt: String(Date.now()),
            ...(jobId ? { lastJobId: jobId } : {})
        })
    }

    async getJob(jobId: string): Promise<Record<string, any> | null> {
        const job = RedisMock.jobs.get(jobId)
        return job ? { ...job } : null
    }

    async getDocStatus(collection: string, docId: _ttid): Promise<Record<string, string> | null> {
        return RedisMock.docs.get(`fylo:doc:${collection}:${docId}`) ?? null
    }

    async readDeadLetters(count: number = 10) {
        return RedisMock.deadLetters.slice(0, count).map(item => ({
            streamId: item.streamId,
            job: { ...RedisMock.jobs.get(item.jobId)! },
            reason: item.reason,
            failedAt: item.failedAt
        }))
    }

    async replayDeadLetter(streamId: string) {
        const item = RedisMock.deadLetters.find(entry => entry.streamId === streamId)
        if(!item) return null

        const job = RedisMock.jobs.get(item.jobId)
        if(!job) return null

        const replayed = {
            ...job,
            status: 'queued',
            error: undefined,
            workerId: undefined,
            attempts: 0,
            updatedAt: Date.now(),
            nextAttemptAt: Date.now()
        }

        RedisMock.jobs.set(item.jobId, replayed)
        await this.enqueueWrite(replayed)
        RedisMock.deadLetters = RedisMock.deadLetters.filter(entry => entry.streamId !== streamId)

        return { ...replayed }
    }

    async getQueueStats() {
        return {
            queued: RedisMock.stream.length,
            pending: RedisMock.stream.filter(entry => entry.claimedBy).length,
            deadLetters: RedisMock.deadLetters.length
        }
    }

    async acquireDocLock(collection: string, docId: _ttid, jobId: string) {
        const key = `fylo:lock:${collection}:${docId}`
        if(RedisMock.locks.has(key)) return false
        RedisMock.locks.set(key, jobId)
        return true
    }

    async releaseDocLock(collection: string, docId: _ttid, jobId: string) {
        const key = `fylo:lock:${collection}:${docId}`
        if(RedisMock.locks.get(key) === jobId) RedisMock.locks.delete(key)
    }

    async *subscribe(_collection: string): AsyncGenerator<never, void, unknown> {}
}
