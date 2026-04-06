interface _getDoc {
    [Symbol.asyncIterator]<T>(): AsyncGenerator<_ttid | Record<_ttid, T>, void, unknown>;
    once<T>(): Promise<Record<_ttid, T>>
    onDelete(): AsyncGenerator<_ttid, void, unknown>
}

interface _findDocs {
    [Symbol.asyncIterator]<T>(): AsyncGenerator<_ttid | Record<_ttid, T> | Record<string, _ttid[]> | Record<_ttid, Partial<T>> | undefined, void, unknown>
    once<T>(): Promise<Record<_ttid, T>>
    onDelete(): AsyncGenerator<_ttid, void, unknown>
}

interface _queuedWriteResult {
    jobId: string
    docId: _ttid
    status: 'queued' | 'processing' | 'committed' | 'failed' | 'dead-letter'
}

interface ObjectConstructor {
    appendGroup: (target: Record<string, any>, source: Record<string, any>) => Record<string, any>;
}

interface Console {
    format: (docs: Record<string, any>) => void
}

type _joinDocs<T, U> = _ttid[] | Record<string, _ttid[]> | Record<string,  Record<_ttid, Partial<T | U>>> | Record<`${_ttid}, ${_ttid}`, T | U | (T & U) | (Partial<T> & Partial<U>)>

declare module "@delma/fylo" {

    export default class {

        /**
         * Rolls back all transcations in current instance
         * @deprecated Prefer queued write recovery, dead letters, or compensating writes.
         */
        rollback(): Promise<void>

        /**
         * Executes a SQL query and returns the results.
         * @param SQL The SQL query to execute.
         * @returns The results of the query.
         */
        executeSQL<T extends Record<string, any>, U extends Record<string, any> = {}>(SQL: string): Promise<number | void | any[] | _ttid | Record<any, any>>
        
        /**
         * Creates a new schema for a collection.
         * @param collection The name of the collection.
         */
        static createCollection(collection: string): Promise<void>

        /**
         * Drops an existing schema for a collection.
         * @param collection The name of the collection.
         */
        static dropCollection(collection: string): Promise<void>

        /**
         * Imports data from a URL into a collection.
         * @param collection The name of the collection.
         * @param url The URL of the data to import.
         * @param limit The maximum number of documents to import.
         */
        importBulkData(collection: string, url: URL, limit?: number): Promise<number>

        /**
         * Exports data from a collection to a URL.
         * @param collection The name of the collection.
         * @returns The current data exported from the collection.
         */
        exportBulkData<T extends Record<string, any>>(collection: string): AsyncGenerator<T, void, unknown>

        /**
         * Gets a document from a collection.
         * @param collection The name of the collection.
         * @param _id The ID of the document.
         * @param onlyId Whether to only return the ID of the document.
         * @returns The document or the ID of the document.
         */
        static getDoc(collection: string, _id: _ttid, onlyId: boolean): _getDoc

        /**
         * Puts multiple documents into a collection.
         * @param collection The name of the collection.
         * @param batch The documents to put.
         * @returns The IDs of the documents.
         */
        batchPutData<T extends Record<string, any>>(collection: string, batch: Array<T>): Promise<_ttid[]>

        queuePutData<T extends Record<string, any>>(collection: string, data: Record<_ttid, T> | T): Promise<_queuedWriteResult>

        queuePatchDoc<T extends Record<string, any>>(collection: string, newDoc: Record<_ttid, Partial<T>>, oldDoc?: Record<_ttid, T>): Promise<_queuedWriteResult>

        queueDelDoc(collection: string, _id: _ttid): Promise<_queuedWriteResult>

        getJobStatus(jobId: string): Promise<Record<string, any> | null>

        getDocStatus(collection: string, docId: _ttid): Promise<Record<string, any> | null>

        getDeadLetters(count?: number): Promise<Array<Record<string, any>>>

        getQueueStats(): Promise<{ queued: number, pending: number, deadLetters: number }>

        replayDeadLetter(streamId: string): Promise<Record<string, any> | null>

        processQueuedWrites(count?: number, recover?: boolean): Promise<number>

        /**
         * Puts a document into a collection.
         * @param collection The name of the collection.
         * @param data The document to put.
         * @returns The ID of the document.
         */
        putData<T extends Record<string, any>>(collection: string, data: T): Promise<_ttid>
        putData<T extends Record<string, any>>(collection: string, data: Record<_ttid, T>): Promise<_ttid>
        putData<T extends Record<string, any>>(collection: string, data: T, options: { wait?: true, timeoutMs?: number }): Promise<_ttid>
        putData<T extends Record<string, any>>(collection: string, data: Record<_ttid, T>, options: { wait?: true, timeoutMs?: number }): Promise<_ttid>
        putData<T extends Record<string, any>>(collection: string, data: T, options: { wait: false, timeoutMs?: number }): Promise<_queuedWriteResult>
        putData<T extends Record<string, any>>(collection: string, data: Record<_ttid, T>, options: { wait: false, timeoutMs?: number }): Promise<_queuedWriteResult>

        /**
         * Patches a document in a collection.
         * @param collection The name of the collection.
         * @param newDoc The new document data.
         * @param oldDoc The old document data.
         * @returns The number of documents patched.
         */
        patchDoc<T extends Record<string, any>>(collection: string, newDoc: Record<_ttid, Partial<T>>, oldDoc?: Record<_ttid, T>): Promise<_ttid>
        patchDoc<T extends Record<string, any>>(collection: string, newDoc: Record<_ttid, Partial<T>>, oldDoc: Record<_ttid, T> | undefined, options: { wait?: true, timeoutMs?: number }): Promise<_ttid>
        patchDoc<T extends Record<string, any>>(collection: string, newDoc: Record<_ttid, Partial<T>>, oldDoc: Record<_ttid, T> | undefined, options: { wait: false, timeoutMs?: number }): Promise<_queuedWriteResult>

        /**
         * Patches documents in a collection.
         * @param collection The name of the collection.
         * @param updateSchema The update schema.
         * @returns The number of documents patched.
         */
        patchDocs<T extends Record<string, any>>(collection: string, updateSchema: _storeUpdate<T>): Promise<number>

        /**
         * Deletes a document from a collection.
         * @param collection The name of the collection.
         * @param _id The ID of the document.
         * @returns The number of documents deleted.
         */
        delDoc(collection: string, _id: _ttid): Promise<void>
        delDoc(collection: string, _id: _ttid, options: { wait?: true, timeoutMs?: number }): Promise<void>
        delDoc(collection: string, _id: _ttid, options: { wait: false, timeoutMs?: number }): Promise<_queuedWriteResult>

        /**
         * Deletes documents from a collection.
         * @param collection The name of the collection.
         * @param deleteSchema The delete schema.
         * @returns The number of documents deleted.
         */
        delDocs<T extends Record<string, any>>(collection: string, deleteSchema?: _storeDelete<T>): Promise<number>

        /**
         * Joins documents from two collections.
         * @param join The join schema.
         * @returns The joined documents.
         */
        static joinDocs<T extends Record<string, any>, U extends Record<string, any>>(join: _join<T, U>): Promise<_joinDocs<T, U>>
        
        /**
         * Finds documents in a collection.
         * @param collection The name of the collection.
         * @param query The query schema.
         * @returns The found documents.
         */
        static findDocs<T extends Record<string, any>>(collection: string, query?: _storeQuery<T>): _findDocs
    }
}
