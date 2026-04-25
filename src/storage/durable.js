import { mkdir, open, rename } from 'node:fs/promises'
import path from 'node:path'

/**
 * Writes `data` to `target` with crash-safe durability guarantees.
 *
 * Pattern: write to `<target>.tmp`, fsync the file, rename into place,
 * then fsync the parent directory so the rename itself is durable.
 *
 * After this resolves, the content at `target` survives a crash or
 * power loss on ext4/xfs/APFS (assuming the underlying disk honors fsync).
 *
 * Creates parent directories as needed.
 *
 * @param {string} target
 * @param {string | Uint8Array} data
 * @returns {Promise<void>}
 */
export async function writeDurable(target, data) {
    const dir = path.dirname(target)
    await mkdir(dir, { recursive: true })
    const tmp = `${target}.tmp`
    const fileHandle = await open(tmp, 'w')
    try {
        await fileHandle.writeFile(data)
        await fileHandle.sync()
    } finally {
        await fileHandle.close()
    }
    await rename(tmp, target)
    const dirHandle = await open(dir, 'r')
    try {
        await dirHandle.sync()
    } finally {
        await dirHandle.close()
    }
}
