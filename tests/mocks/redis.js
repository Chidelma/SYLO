export default class RedisMock {
    static stream = []
    static jobs = new Map()
    static docs = new Map()
    static locks = new Map()
    static deadLetters = []
    static nextId = 0
    async publish(_collection, _action, _keyId) {}
    async claimTTID(_id, _ttlSeconds = 10) {
        return true
    }
    async enqueueWrite(job) {
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
    async readWriteJobs(workerId, count = 1) {
        const available = RedisMock.stream.filter((entry) => !entry.claimedBy).slice(0, count)
        for (const entry of available) entry.claimedBy = workerId
        return available.map((entry) => ({
            streamId: entry.streamId,
            job: { ...RedisMock.jobs.get(entry.jobId) }
        }))
    }
    async ackWriteJob(streamId) {
        RedisMock.stream = RedisMock.stream.filter((item) => item.streamId !== streamId)
    }
    async deadLetterWriteJob(streamId, job, reason) {
        RedisMock.deadLetters.push({
            streamId: String(RedisMock.deadLetters.length + 1),
            jobId: job.jobId,
            reason,
            failedAt: Date.now()
        })
        await this.ackWriteJob(streamId)
    }
    async claimPendingJobs(workerId, _minIdleMs = 30000, count = 10) {
        const pending = RedisMock.stream.filter((entry) => entry.claimedBy).slice(0, count)
        for (const entry of pending) entry.claimedBy = workerId
        return pending.map((entry) => ({
            streamId: entry.streamId,
            job: { ...RedisMock.jobs.get(entry.jobId) }
        }))
    }
    async setJobStatus(jobId, status, extra = {}) {
        const job = RedisMock.jobs.get(jobId)
        if (job) Object.assign(job, extra, { status, updatedAt: Date.now() })
    }
    async setDocStatus(collection, docId, status, jobId) {
        const key = `fylo:doc:${collection}:${docId}`
        const curr = RedisMock.docs.get(key) ?? {}
        RedisMock.docs.set(key, {
            ...curr,
            status,
            updatedAt: String(Date.now()),
            ...(jobId ? { lastJobId: jobId } : {})
        })
    }
    async getJob(jobId) {
        const job = RedisMock.jobs.get(jobId)
        return job ? { ...job } : null
    }
    async getDocStatus(collection, docId) {
        return RedisMock.docs.get(`fylo:doc:${collection}:${docId}`) ?? null
    }
    async readDeadLetters(count = 10) {
        return RedisMock.deadLetters.slice(0, count).map((item) => ({
            streamId: item.streamId,
            job: { ...RedisMock.jobs.get(item.jobId) },
            reason: item.reason,
            failedAt: item.failedAt
        }))
    }
    async replayDeadLetter(streamId) {
        const item = RedisMock.deadLetters.find((entry) => entry.streamId === streamId)
        if (!item) return null
        const job = RedisMock.jobs.get(item.jobId)
        if (!job) return null
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
        RedisMock.deadLetters = RedisMock.deadLetters.filter((entry) => entry.streamId !== streamId)
        return { ...replayed }
    }
    async getQueueStats() {
        return {
            queued: RedisMock.stream.length,
            pending: RedisMock.stream.filter((entry) => entry.claimedBy).length,
            deadLetters: RedisMock.deadLetters.length
        }
    }
    async acquireDocLock(collection, docId, jobId) {
        const key = `fylo:lock:${collection}:${docId}`
        if (RedisMock.locks.has(key)) return false
        RedisMock.locks.set(key, jobId)
        return true
    }
    async releaseDocLock(collection, docId, jobId) {
        const key = `fylo:lock:${collection}:${docId}`
        if (RedisMock.locks.get(key) === jobId) RedisMock.locks.delete(key)
    }
    async *subscribe(_collection) {}
}
