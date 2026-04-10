import { mkdir, open, readFile, readdir, rm, stat, writeFile } from 'node:fs/promises'
import path from 'node:path'
import type { EventBus, LockManager, StorageEngine } from '../types'
import type { S3FilesEvent } from './types'

export class FilesystemStorage implements StorageEngine {
    async read(target: string): Promise<string> {
        return await readFile(target, 'utf8')
    }

    async write(target: string, data: string): Promise<void> {
        await mkdir(path.dirname(target), { recursive: true })
        await writeFile(target, data, 'utf8')
    }

    async delete(target: string): Promise<void> {
        await rm(target, { recursive: true, force: true })
    }

    async list(target: string): Promise<string[]> {
        const results: string[] = []

        try {
            const entries = await readdir(target, { withFileTypes: true })
            for (const entry of entries) {
                const child = path.join(target, entry.name)
                if (entry.isDirectory()) {
                    results.push(...(await this.list(child)))
                } else {
                    results.push(child)
                }
            }
        } catch (err) {
            if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err
        }

        return results
    }

    async mkdir(target: string): Promise<void> {
        await mkdir(target, { recursive: true })
    }

    async rmdir(target: string): Promise<void> {
        await rm(target, { recursive: true, force: true })
    }

    async exists(target: string): Promise<boolean> {
        try {
            await stat(target)
            return true
        } catch (err) {
            if ((err as NodeJS.ErrnoException).code === 'ENOENT') return false
            throw err
        }
    }
}

export class FilesystemLockManager implements LockManager {
    constructor(
        private readonly root: string,
        private readonly storage: StorageEngine
    ) {}

    private lockDir(collection: string, docId: _ttid) {
        return path.join(this.root, collection, '.fylo', 'locks', `${docId}.lock`)
    }

    async acquire(
        collection: string,
        docId: _ttid,
        owner: string,
        ttlMs: number = 30_000
    ): Promise<boolean> {
        const dir = this.lockDir(collection, docId)
        const metaPath = path.join(dir, 'meta.json')
        await mkdir(path.dirname(dir), { recursive: true })

        try {
            await mkdir(dir, { recursive: false })
            await this.storage.write(metaPath, JSON.stringify({ owner, ts: Date.now() }))
            return true
        } catch (err) {
            if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err
        }

        try {
            const meta = JSON.parse(await this.storage.read(metaPath)) as { ts?: number }
            if (meta.ts && Date.now() - meta.ts > ttlMs) {
                await this.storage.rmdir(dir)
                await mkdir(dir, { recursive: false })
                await this.storage.write(metaPath, JSON.stringify({ owner, ts: Date.now() }))
                return true
            }
        } catch {
            await this.storage.rmdir(dir)
            await mkdir(dir, { recursive: false })
            await this.storage.write(metaPath, JSON.stringify({ owner, ts: Date.now() }))
            return true
        }

        return false
    }

    async release(collection: string, docId: _ttid, owner: string): Promise<void> {
        const dir = this.lockDir(collection, docId)
        const metaPath = path.join(dir, 'meta.json')

        try {
            const meta = JSON.parse(await this.storage.read(metaPath)) as { owner?: string }
            if (meta.owner === owner) await this.storage.rmdir(dir)
        } catch (err) {
            if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err
        }
    }
}

export class FilesystemEventBus<T extends Record<string, any>> implements EventBus<
    S3FilesEvent<T>
> {
    constructor(
        private readonly root: string,
        private readonly storage: StorageEngine
    ) {}

    private journalPath(collection: string) {
        return path.join(this.root, collection, '.fylo', 'events', `${collection}.ndjson`)
    }

    async publish(collection: string, event: S3FilesEvent<T>): Promise<void> {
        const target = this.journalPath(collection)
        await mkdir(path.dirname(target), { recursive: true })
        const line = `${JSON.stringify(event)}\n`
        const handle = await open(target, 'a')
        try {
            await handle.write(line)
        } finally {
            await handle.close()
        }
    }

    async *listen(collection: string): AsyncGenerator<S3FilesEvent<T>, void, unknown> {
        const target = this.journalPath(collection)
        let position = 0

        while (true) {
            try {
                const fileStat = await stat(target)
                if (fileStat.size > position) {
                    const handle = await open(target, 'r')
                    try {
                        const size = fileStat.size - position
                        const buffer = Buffer.alloc(size)
                        await handle.read(buffer, 0, size, position)
                        position = fileStat.size

                        for (const line of buffer.toString('utf8').split('\n')) {
                            if (line.trim().length === 0) continue
                            yield JSON.parse(line) as S3FilesEvent<T>
                        }
                    } finally {
                        await handle.close()
                    }
                }
            } catch (err) {
                if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err
            }

            await Bun.sleep(100)
        }
    }
}
