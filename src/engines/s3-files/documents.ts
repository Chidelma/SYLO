import path from 'node:path'
import TTID from '@d31ma/ttid'
import { assertPathInside, validateDocId } from '../../core/doc-id'
import type { StorageEngine } from '../types'
import type { StoredDoc, StoredHead, StoredVersionMeta } from './types'

export class S3FilesDocuments {
    constructor(
        private readonly storage: StorageEngine,
        private readonly docsRoot: (collection: string) => string,
        private readonly docPath: (collection: string, docId: _ttid) => string,
        private readonly headsRoot: (collection: string) => string,
        private readonly headPath: (collection: string, lineageId: string) => string,
        private readonly versionsRoot: (collection: string) => string,
        private readonly versionMetaPath: (collection: string, docId: _ttid) => string,
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
        validateDocId(docId)
        const target = this.docPath(collection, docId)
        assertPathInside(this.docsRoot(collection), target)

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
        validateDocId(docId)
        await this.ensureCollection(collection)
        const encoded = await this.encodeEncrypted(collection, data)
        const target = this.docPath(collection, docId)
        assertPathInside(this.docsRoot(collection), target)
        await this.storage.write(target, JSON.stringify(encoded))
    }

    async removeStoredDoc(collection: string, docId: _ttid) {
        validateDocId(docId)
        const target = this.docPath(collection, docId)
        assertPathInside(this.docsRoot(collection), target)
        await this.storage.delete(target)
    }

    async readVersionMeta(collection: string, docId: _ttid): Promise<StoredVersionMeta | null> {
        validateDocId(docId)
        const target = this.versionMetaPath(collection, docId)
        assertPathInside(this.versionsRoot(collection), target)

        try {
            return JSON.parse(await this.storage.read(target)) as StoredVersionMeta
        } catch (err) {
            if ((err as NodeJS.ErrnoException).code === 'ENOENT') return null
            throw err
        }
    }

    async writeVersionMeta(collection: string, meta: StoredVersionMeta) {
        validateDocId(meta.versionId)
        await this.ensureCollection(collection)
        const target = this.versionMetaPath(collection, meta.versionId)
        assertPathInside(this.versionsRoot(collection), target)
        await this.storage.write(target, JSON.stringify(meta))
    }

    async readHead(collection: string, lineageId: string): Promise<StoredHead | null> {
        const target = this.headPath(collection, lineageId)
        assertPathInside(this.headsRoot(collection), target)

        try {
            return JSON.parse(await this.storage.read(target)) as StoredHead
        } catch (err) {
            if ((err as NodeJS.ErrnoException).code === 'ENOENT') return null
            throw err
        }
    }

    async writeHead(collection: string, head: StoredHead) {
        await this.ensureCollection(collection)
        const target = this.headPath(collection, head.lineageId)
        assertPathInside(this.headsRoot(collection), target)
        await this.storage.write(target, JSON.stringify(head))
    }

    async resolveHead(collection: string, docId: _ttid): Promise<StoredHead | null> {
        const directHead = await this.readHead(collection, docId)
        if (directHead) return directHead

        const meta = await this.readVersionMeta(collection, docId)
        if (!meta) return null

        return await this.readHead(collection, meta.lineageId)
    }

    async listDocIds(collection: string) {
        const files = await this.storage.list(this.docsRoot(collection))
        return files
            .filter((file) => file.endsWith('.json'))
            .map((file) => path.basename(file, '.json'))
            .filter((key) => TTID.isTTID(key)) as _ttid[]
    }

    async listActiveDocIds(collection: string) {
        const files = await this.storage.list(this.headsRoot(collection))
        const ids: _ttid[] = []

        for (const file of files) {
            if (!file.endsWith('.json')) continue
            const head = JSON.parse(await this.storage.read(file)) as StoredHead
            if (head.deleted || !TTID.isTTID(head.currentVersionId)) continue
            ids.push(head.currentVersionId)
        }

        return ids
    }
}
