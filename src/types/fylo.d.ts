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

type _fyloAuthAction =
    | 'collection:create'
    | 'collection:drop'
    | 'doc:read'
    | 'doc:find'
    | 'doc:create'
    | 'doc:update'
    | 'doc:delete'
    | 'bulk:import'
    | 'bulk:export'
    | 'join:execute'
    | 'sql:execute'

interface _fyloAuthContext {
    subjectId: string
    tenantId?: string
    roles?: string[]
    [key: string]: unknown
}

interface _fyloAuthorizeInput {
    auth: _fyloAuthContext
    action: _fyloAuthAction
    collection?: string
    collections?: string[]
    docId?: string
    data?: unknown
    query?: unknown
    sql?: string
}

interface _fyloAuthPolicy {
    authorize(input: _fyloAuthorizeInput): boolean | Promise<boolean>
}

interface _fyloOptions {
    root?: string
    s3FilesRoot?: string
    auth?: _fyloAuthPolicy
    sync?: _fyloSyncHooks
    syncMode?: _fyloSyncMode
}

interface _importBulkDataOptions {
    limit?: number
    maxBytes?: number
    allowedProtocols?: string[]
    allowedHosts?: string[]
    allowPrivateNetwork?: boolean
}

declare module '@d31ma/fylo' {
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
    export type FyloAuthAction = _fyloAuthAction
    export type FyloAuthContext = _fyloAuthContext
    export type FyloAuthorizeInput = _fyloAuthorizeInput
    export type FyloAuthPolicy = _fyloAuthPolicy
    export type ImportBulkDataOptions = _importBulkDataOptions

    export class FyloAuthError extends Error {
        readonly action: _fyloAuthAction
        readonly collection?: string
        readonly docId?: string
    }

    export class AuthenticatedFylo {
        rollback(): Promise<void>
        createCollection(collection: string): Promise<void>
        dropCollection(collection: string): Promise<void>
        getDoc(collection: string, _id: _ttid, onlyId?: boolean): _getDoc
        findDocs<T extends Record<string, any>>(
            collection: string,
            query?: _storeQuery<T>
        ): _findDocs
        joinDocs<T extends Record<string, any>, U extends Record<string, any>>(
            join: _join<T, U>
        ): Promise<_joinDocs<T, U>>
        exportBulkData<T extends Record<string, any>>(
            collection: string
        ): AsyncGenerator<T, void, unknown>
        importBulkData(
            collection: string,
            url: URL,
            limitOrOptions?: number | _importBulkDataOptions
        ): Promise<number>
        executeSQL<T extends Record<string, any>, U extends Record<string, any> = {}>(
            SQL: string
        ): Promise<number | void | any[] | _ttid | Record<any, any>>
        batchPutData<T extends Record<string, any>>(
            collection: string,
            batch: Array<T>
        ): Promise<_ttid[]>
        putData<T extends Record<string, any>>(collection: string, data: T): Promise<_ttid>
        putData<T extends Record<string, any>>(
            collection: string,
            data: Record<_ttid, T>
        ): Promise<_ttid>
        putData<T extends Record<string, any>>(
            collection: string,
            data: Record<_ttid, T> | T,
            options?: { wait?: boolean; timeoutMs?: number }
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
    }

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

        as(auth: _fyloAuthContext, policy?: _fyloAuthPolicy): AuthenticatedFylo

        importBulkData(
            collection: string,
            url: URL,
            limitOrOptions?: number | _importBulkDataOptions
        ): Promise<number>

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
        putData<T extends Record<string, any>>(
            collection: string,
            data: Record<_ttid, T> | T,
            options?: { wait?: boolean; timeoutMs?: number }
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
