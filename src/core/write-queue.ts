import type { WriteJob } from '../types/write-queue'

export class WriteQueue {

    static createInsertJob<T extends Record<string, any>>(collection: string, docId: _ttid, payload: T): WriteJob<T> {
        const now = Date.now()

        return {
            jobId: Bun.randomUUIDv7(),
            collection,
            docId,
            operation: 'insert',
            payload,
            status: 'queued',
            attempts: 0,
            createdAt: now,
            updatedAt: now
        }
    }

    static createUpdateJob<T extends Record<string, any>>(
        collection: string,
        docId: _ttid,
        payload: { newDoc: Record<_ttid, Partial<T>>, oldDoc?: Record<_ttid, T> }
    ): WriteJob<{ newDoc: Record<_ttid, Partial<T>>, oldDoc?: Record<_ttid, T> }> {
        const now = Date.now()

        return {
            jobId: Bun.randomUUIDv7(),
            collection,
            docId,
            operation: 'update',
            payload,
            status: 'queued',
            attempts: 0,
            createdAt: now,
            updatedAt: now
        }
    }

    static createDeleteJob(collection: string, docId: _ttid): WriteJob<{ _id: _ttid }> {
        const now = Date.now()

        return {
            jobId: Bun.randomUUIDv7(),
            collection,
            docId,
            operation: 'delete',
            payload: { _id: docId },
            status: 'queued',
            attempts: 0,
            createdAt: now,
            updatedAt: now
        }
    }
}
