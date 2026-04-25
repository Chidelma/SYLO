import { link, mkdir, rename, unlink, writeFile } from 'node:fs/promises'
import path from 'node:path'

/** @type {Map<string, { interval: ReturnType<typeof setInterval>, owner: string }>} */
const heartbeats = new Map()

/**
 * @param {string} lockPath
 * @param {string} owner
 * @param {number} ttlMs
 */
function startHeartbeat(lockPath, owner, ttlMs) {
    stopHeartbeat(lockPath)
    const intervalMs = Math.max(Math.floor(ttlMs / 3), 100)
    const interval = setInterval(async () => {
        try {
            const meta = await readLockMeta(lockPath)
            if (!meta || meta.owner !== owner) {
                stopHeartbeat(lockPath)
                return
            }
            const tmp = `${lockPath}.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}.heartbeat.tmp`
            await writeFile(tmp, JSON.stringify({ owner, ts: Date.now() }))
            try {
                await rename(tmp, lockPath)
            } catch (err) {
                try {
                    await unlink(tmp)
                } catch {}
                throw err
            }
        } catch {}
    }, intervalMs)
    if (typeof interval.unref === 'function') interval.unref()
    heartbeats.set(lockPath, { interval, owner })
}

/** @param {string} lockPath */
function stopHeartbeat(lockPath) {
    const entry = heartbeats.get(lockPath)
    if (!entry) return
    clearInterval(entry.interval)
    heartbeats.delete(lockPath)
}

/**
 * Reads and parses a lock file's JSON payload.
 * Returns null if the file is missing or unreadable; returns the parsed
 * object otherwise. Corrupt JSON yields null (treated as stale).
 *
 * @param {string} lockPath
 * @returns {Promise<{ owner: string, ts: number } | null>}
 */
async function readLockMeta(lockPath) {
    try {
        const raw = await Bun.file(lockPath).text()
        return JSON.parse(raw)
    } catch (err) {
        const error = /** @type {NodeJS.ErrnoException} */ (err)
        if (error.code === 'ENOENT') return null
        return null
    }
}

/**
 * Attempts an atomic create-exclusive of the lock file with the given payload.
 * Uses `link()` rather than `open(wx)` so the file appears at `lockPath` with
 * its content already populated — this closes the race where a concurrent
 * reader sees an empty file after `open(wx)` but before `write`.
 *
 * Resolves true on success; resolves false if the target already existed.
 * Any other error rethrows.
 *
 * @param {string} lockPath
 * @param {string} payload
 */
async function tryCreateExclusive(lockPath, payload) {
    const tmp = `${lockPath}.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}.tmp`
    await writeFile(tmp, payload)
    try {
        await link(tmp, lockPath)
        return true
    } catch (err) {
        const error = /** @type {NodeJS.ErrnoException} */ (err)
        if (error.code === 'EEXIST') return false
        throw err
    } finally {
        try {
            await unlink(tmp)
        } catch (cleanupErr) {
            const cleanupError = /** @type {NodeJS.ErrnoException} */ (cleanupErr)
            if (cleanupError.code !== 'ENOENT') throw cleanupErr
        }
    }
}

/**
 * @typedef {object} TryAcquireFileLockOptions
 * @property {number=} ttlMs
 * @property {(info: { lockPath: string, newOwner: string, previousOwner?: string }) => void=} onTakeover
 *   Invoked after a stale lock is successfully reclaimed. Not called for
 *   live-lock rejections or lost takeover races.
 * @property {boolean=} heartbeat
 *   When true, refresh the lock's timestamp every `ttlMs/3` while held so
 *   long-running operations are not misclassified as stale. Stopped by
 *   `tryReleaseFileLock`. Off by default; enable for collection-scope
 *   locks held for the duration of bulk writes or rebuilds.
 */

/**
 * Acquires an advisory file-based lock.
 *
 * Semantics:
 * - Atomic `wx` open is the only path by which ownership is established —
 *   the filesystem guarantees at most one concurrent acquirer wins.
 * - If the lock already exists and its timestamp is within `ttlMs`, the
 *   current holder is considered live and this call returns false.
 * - If the lock is stale (or its payload is missing/corrupt), a single
 *   takeover attempt is made: unlink + retry `wx`. The loser of a
 *   concurrent takeover race returns false and should retry at the
 *   caller layer.
 *
 * Release is only safe if the holder completes their work before `ttlMs`
 * elapses. Callers should choose a TTL comfortably larger than their
 * longest expected operation.
 *
 * @param {string} lockPath
 * @param {string} owner
 * @param {number | TryAcquireFileLockOptions} [ttlMsOrOptions]
 * @returns {Promise<boolean>}
 */
export async function tryAcquireFileLock(lockPath, owner, ttlMsOrOptions = 30_000) {
    const options = typeof ttlMsOrOptions === 'number' ? { ttlMs: ttlMsOrOptions } : ttlMsOrOptions
    const ttlMs = options.ttlMs ?? 30_000
    await mkdir(path.dirname(lockPath), { recursive: true })
    const payload = JSON.stringify({ owner, ts: Date.now() })
    if (await tryCreateExclusive(lockPath, payload)) {
        if (options.heartbeat) startHeartbeat(lockPath, owner, ttlMs)
        return true
    }
    const meta = await readLockMeta(lockPath)
    if (meta && typeof meta.ts === 'number' && Date.now() - meta.ts <= ttlMs) {
        return false
    }
    const previousOwner = meta && typeof meta.owner === 'string' ? meta.owner : undefined
    try {
        await unlink(lockPath)
    } catch (err) {
        const error = /** @type {NodeJS.ErrnoException} */ (err)
        if (error.code !== 'ENOENT') throw err
    }
    const acquired = await tryCreateExclusive(lockPath, payload)
    if (acquired) {
        if (options.onTakeover) {
            try {
                options.onTakeover({ lockPath, newOwner: owner, previousOwner })
            } catch (err) {
                console.error('FYLO onTakeover callback threw:', err)
            }
        }
        if (options.heartbeat) startHeartbeat(lockPath, owner, ttlMs)
    }
    return acquired
}

/**
 * Blocking variant of `tryAcquireFileLock`: polls with exponential backoff
 * (capped) until the lock is acquired or `waitTimeoutMs` elapses. Throws
 * on timeout. `ttlMs` controls stale-lock takeover; see `tryAcquireFileLock`.
 *
 * @param {string} lockPath
 * @param {string} owner
 * @param {object} [options]
 * @param {number} [options.ttlMs]
 * @param {number} [options.waitTimeoutMs]
 * @param {boolean} [options.heartbeat]
 * @param {(info: { lockPath: string, newOwner: string, previousOwner?: string }) => void} [options.onTakeover]
 * @returns {Promise<void>}
 */
export async function waitAcquireFileLock(lockPath, owner, options = {}) {
    const ttlMs = options.ttlMs ?? 30_000
    const waitTimeoutMs = options.waitTimeoutMs ?? 60_000
    const onTakeover = options.onTakeover
    const heartbeat = options.heartbeat ?? false
    const deadline = Date.now() + waitTimeoutMs
    let delay = 2
    while (true) {
        if (await tryAcquireFileLock(lockPath, owner, { ttlMs, onTakeover, heartbeat })) return
        if (Date.now() >= deadline) {
            throw new Error(`Timed out waiting for filesystem lock at ${lockPath}`)
        }
        await Bun.sleep(delay)
        delay = Math.min(delay * 2, 100)
    }
}

/**
 * Releases the lock at `lockPath` if (and only if) the current payload
 * names this owner. Missing lock files are silently ignored.
 *
 * Note: release is not atomic with the ownership check. A concurrent
 * stale-lock takeover between our read and unlink could cause us to
 * delete someone else's lock. This is acceptable for FYLO's advisory
 * locking — callers rely on short operation durations plus TTL-based
 * correctness, not fine-grained release ordering.
 *
 * @param {string} lockPath
 * @param {string} owner
 * @returns {Promise<void>}
 */
export async function tryReleaseFileLock(lockPath, owner) {
    stopHeartbeat(lockPath)
    const meta = await readLockMeta(lockPath)
    if (!meta || meta.owner !== owner) return
    try {
        await unlink(lockPath)
    } catch (err) {
        const error = /** @type {NodeJS.ErrnoException} */ (err)
        if (error.code !== 'ENOENT') throw err
    }
}
