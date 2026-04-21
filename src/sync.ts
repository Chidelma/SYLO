import type { FyloAuthPolicy } from './auth'

export type FyloSyncMode = 'await-sync' | 'fire-and-forget'
export type FyloWormMode = 'off' | 'append-only'

export interface FyloWormOptions {
    mode?: FyloWormMode
    deletePolicy?: 'reject' | 'tombstone'
}

export interface FyloWormWriteSyncInfo {
    lineageId: _ttid
    headOperation: 'create' | 'advance'
    headDocId: _ttid
    headPath: string
}

export interface FyloWormDeleteSyncInfo {
    lineageId: _ttid
    headOperation: 'delete'
    headDocId: _ttid
    headPath: string
    deleteMode: 'physical' | 'tombstone'
    versionPath?: string
}

export interface FyloWriteSyncEvent<T extends Record<string, any> = Record<string, any>> {
    operation: 'put' | 'patch'
    collection: string
    docId: _ttid
    previousDocId?: _ttid
    path: string
    data: T
    worm?: FyloWormWriteSyncInfo
}

export interface FyloDeleteSyncEvent {
    operation: 'delete' | 'patch'
    collection: string
    docId: _ttid
    path: string
    worm?: FyloWormDeleteSyncInfo
}

export interface FyloSyncHooks<T extends Record<string, any> = Record<string, any>> {
    onWrite?: (event: FyloWriteSyncEvent<T>) => Promise<void> | void
    onDelete?: (event: FyloDeleteSyncEvent) => Promise<void> | void
}

export interface FyloOptions<T extends Record<string, any> = Record<string, any>> {
    root?: string
    s3FilesRoot?: string
    auth?: FyloAuthPolicy
    sync?: FyloSyncHooks<T>
    syncMode?: FyloSyncMode
    worm?: FyloWormOptions
}

export class FyloSyncError extends Error {
    readonly collection: string
    readonly docId: _ttid
    readonly path: string
    readonly operation: string

    constructor(args: {
        collection: string
        docId: _ttid
        path: string
        operation: string
        cause: unknown
    }) {
        super(
            `FYLO sync failed after the local filesystem operation succeeded for ${args.operation} ${args.collection}/${args.docId}. Local state is already committed at ${args.path}.`,
            { cause: args.cause }
        )
        this.name = 'FyloSyncError'
        this.collection = args.collection
        this.docId = args.docId
        this.path = args.path
        this.operation = args.operation
    }
}

export function resolveSyncMode(syncMode?: FyloSyncMode): FyloSyncMode {
    return syncMode ?? 'await-sync'
}
