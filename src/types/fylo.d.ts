interface _getDoc {
    [Symbol.asyncIterator]<T>(): AsyncGenerator<_ttid | Record<_ttid, T>, void, unknown>
    once<T>(): Promise<Record<_ttid, T>>
    onDelete(): AsyncGenerator<_ttid, void, unknown>
}

interface _findDocs {
    [Symbol.asyncIterator]<T>(): AsyncGenerator<
        _ttid | Record<_ttid, T> | Record<string, _ttid[]> | Record<_ttid, Partial<T>> | undefined,
        void,
        unknown
    >
    collect<T>(): AsyncGenerator<
        _ttid | Record<_ttid, T> | Record<string, _ttid[]> | Record<_ttid, Partial<T>> | undefined,
        void,
        unknown
    >
    onDelete(): AsyncGenerator<_ttid, void, unknown>
}

interface ObjectConstructor {
    appendGroup: (target: Record<string, any>, source: Record<string, any>) => Record<string, any>
}

interface Console {
    format: (docs: Record<string, any>) => void
}

type _joinDocs<T, U> =
    | _ttid[]
    | Record<string, _ttid[]>
    | Record<string, Record<_ttid, Partial<T | U>>>
    | Record<`${_ttid}, ${_ttid}`, T | U | (T & U) | (Partial<T> & Partial<U>)>

type _fyloSyncMode = 'await-sync' | 'fire-and-forget'

interface _fyloWriteSyncEvent<T extends Record<string, any> = Record<string, any>> {
    operation: 'put' | 'patch'
    collection: string
    docId: _ttid
    previousDocId?: _ttid
    path: string
    data: T
}

interface _fyloDeleteSyncEvent {
    operation: 'delete' | 'patch'
    collection: string
    docId: _ttid
    path: string
}

interface _fyloSyncHooks<T extends Record<string, any> = Record<string, any>> {
    onWrite?: (event: _fyloWriteSyncEvent<T>) => Promise<void> | void
    onDelete?: (event: _fyloDeleteSyncEvent) => Promise<void> | void
}

interface _fyloOptions {
    root?: string
    s3FilesRoot?: string
    sync?: _fyloSyncHooks
    syncMode?: _fyloSyncMode
}

declare module '@delma/fylo' {
    export class FyloSyncError extends Error {
        readonly collection: string
        readonly docId: _ttid
        readonly path: string
        readonly operation: string
    }

    export type FyloSyncMode = _fyloSyncMode
    export type FyloWriteSyncEvent<T extends Record<string, any> = Record<string, any>> =
        _fyloWriteSyncEvent<T>
    export type FyloDeleteSyncEvent = _fyloDeleteSyncEvent
    export type FyloSyncHooks<T extends Record<string, any> = Record<string, any>> =
        _fyloSyncHooks<T>
    export type FyloOptions = _fyloOptions

    export default class {
        constructor(options?: _fyloOptions)

        /**
         * Compatibility helper. FYLO now writes synchronously to the filesystem,
         * so rollback is a no-op.
         */
        rollback(): Promise<void>

        /**
         * Executes a SQL query and returns the results.
         * @param SQL The SQL query to execute.
         * @returns The results of the query.
         */
        executeSQL<T extends Record<string, any>, U extends Record<string, any> = {}>(
            SQL: string
        ): Promise<number | void | any[] | _ttid | Record<any, any>>

        static createCollection(collection: string): Promise<void>
        static dropCollection(collection: string): Promise<void>

        createCollection(collection: string): Promise<void>
        dropCollection(collection: string): Promise<void>

        importBulkData(collection: string, url: URL, limit?: number): Promise<number>

        exportBulkData<T extends Record<string, any>>(
            collection: string
        ): AsyncGenerator<T, void, unknown>

        static exportBulkData<T extends Record<string, any>>(
            collection: string
        ): AsyncGenerator<T, void, unknown>

        static getDoc(collection: string, _id: _ttid, onlyId?: boolean): _getDoc

        getDoc(collection: string, _id: _ttid, onlyId?: boolean): _getDoc

        batchPutData<T extends Record<string, any>>(
            collection: string,
            batch: Array<T>
        ): Promise<_ttid[]>

        putData<T extends Record<string, any>>(collection: string, data: T): Promise<_ttid>
        putData<T extends Record<string, any>>(
            collection: string,
            data: Record<_ttid, T>
        ): Promise<_ttid>

        patchDoc<T extends Record<string, any>>(
            collection: string,
            newDoc: Record<_ttid, Partial<T>>,
            oldDoc?: Record<_ttid, T>
        ): Promise<_ttid>

        patchDocs<T extends Record<string, any>>(
            collection: string,
            updateSchema: _storeUpdate<T>
        ): Promise<number>

        delDoc(collection: string, _id: _ttid): Promise<void>

        delDocs<T extends Record<string, any>>(
            collection: string,
            deleteSchema?: _storeDelete<T>
        ): Promise<number>

        static joinDocs<T extends Record<string, any>, U extends Record<string, any>>(
            join: _join<T, U>
        ): Promise<_joinDocs<T, U>>

        joinDocs<T extends Record<string, any>, U extends Record<string, any>>(
            join: _join<T, U>
        ): Promise<_joinDocs<T, U>>

        static findDocs<T extends Record<string, any>>(
            collection: string,
            query?: _storeQuery<T>
        ): _findDocs

        findDocs<T extends Record<string, any>>(
            collection: string,
            query?: _storeQuery<T>
        ): _findDocs
    }
}
