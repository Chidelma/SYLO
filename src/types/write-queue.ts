export type WriteJobOperation = 'insert' | 'update' | 'delete'

export type WriteJobStatus = 'queued' | 'processing' | 'committed' | 'failed' | 'dead-letter'

export interface WriteJob<T extends Record<string, any> = Record<string, any>> {
    jobId: string
    collection: string
    docId: _ttid
    operation: WriteJobOperation
    payload: T
    status: WriteJobStatus
    attempts: number
    createdAt: number
    updatedAt: number
    nextAttemptAt?: number
    workerId?: string
    error?: string
}

export interface QueuedWriteResult {
    jobId: string
    docId: _ttid
    status: WriteJobStatus
}

export interface StreamJobEntry<T extends Record<string, any> = Record<string, any>> {
    streamId: string
    job: WriteJob<T>
}

export interface DeadLetterJob<T extends Record<string, any> = Record<string, any>> {
    streamId: string
    job: WriteJob<T>
    reason?: string
    failedAt: number
}

export interface QueueStats {
    queued: number
    pending: number
    deadLetters: number
}
