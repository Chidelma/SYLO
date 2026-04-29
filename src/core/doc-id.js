import path from 'node:path'
import TTID from '@d31ma/ttid'

/**
 * @param {string} docId
 */
export function validateDocId(docId) {
    if (!TTID.isTTID(docId)) throw new Error(`Invalid document ID: ${docId}`)
}

/**
 * @param {string} parent
 * @param {string} target
 */
export function assertPathInside(parent, target) {
    const resolvedParent = path.resolve(parent)
    const resolvedTarget = path.resolve(target)
    const relative = path.relative(resolvedParent, resolvedTarget)
    if (relative.startsWith('..') || path.isAbsolute(relative)) {
        throw new Error(`Unsafe document path: ${target}`)
    }
}
