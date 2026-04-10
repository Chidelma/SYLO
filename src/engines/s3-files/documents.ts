import path from 'node:path'
import TTID from '@delma/ttid'
import type { StorageEngine } from '../types'
import type { StoredDoc } from './types'

export class S3FilesDocuments {
    constructor(
        private readonly storage: StorageEngine,
        private readonly docsRoot: (collection: string) => string,
        private readonly docPath: (collection: string, docId: _ttid) => string,
        private readonly ensureCollection: (collection: string) => Promise<void>,
        private readonly encodeEncrypted: <T extends Record<string, any>>(
            collection: string,
            value: T,
            parentField?: string
        ) => Promise<T>,
        private readonly decodeEncrypted: <T extends Record<string, any>>(
            collection: string,
            value: T,
            parentField?: string
        ) => Promise<T>
    ) {}

    async readStoredDoc<T extends Record<string, any>>(
        collection: string,
        docId: _ttid
    ): Promise<StoredDoc<T> | null> {
        const target = this.docPath(collection, docId)

        try {
            const raw = JSON.parse(await this.storage.read(target)) as T
            const decoded = await this.decodeEncrypted(collection, raw)
            const { createdAt, updatedAt } = TTID.decodeTime(docId)

            return {
                id: docId,
                createdAt,
                updatedAt: updatedAt ?? createdAt,
                data: decoded
            }
        } catch (err) {
            if ((err as NodeJS.ErrnoException).code === 'ENOENT') return null
            throw err
        }
    }

    async writeStoredDoc<T extends Record<string, any>>(collection: string, docId: _ttid, data: T) {
        await this.ensureCollection(collection)
        const encoded = await this.encodeEncrypted(collection, data)
        const target = this.docPath(collection, docId)
        await this.storage.write(target, JSON.stringify(encoded))
    }

    async removeStoredDoc(collection: string, docId: _ttid) {
        await this.storage.delete(this.docPath(collection, docId))
    }

    async listDocIds(collection: string) {
        const files = await this.storage.list(this.docsRoot(collection))
        return files
            .filter((file) => file.endsWith('.json'))
            .map((file) => path.basename(file, '.json'))
            .filter((key) => TTID.isTTID(key)) as _ttid[]
    }
}
