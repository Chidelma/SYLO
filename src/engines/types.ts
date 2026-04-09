export type FyloStorageEngineKind = 'legacy-s3' | 's3-files'

export interface StorageEngine {
    read(path: string): Promise<string>
    write(path: string, data: string): Promise<void>
    delete(path: string): Promise<void>
    list(path: string): Promise<string[]>
    mkdir(path: string): Promise<void>
    rmdir(path: string): Promise<void>
    exists(path: string): Promise<boolean>
}

export interface LockManager {
    acquire(collection: string, docId: _ttid, owner: string, ttlMs?: number): Promise<boolean>
    release(collection: string, docId: _ttid, owner: string): Promise<void>
}

export interface EventBus<T> {
    publish(collection: string, event: T): Promise<void>
    listen(collection: string): AsyncGenerator<T, void, unknown>
}
