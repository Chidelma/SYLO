import path from 'node:path'
import TTID from '@d31ma/ttid'

export function validateDocId(docId: string): asserts docId is _ttid {
    if (!TTID.isTTID(docId)) throw new Error(`Invalid document ID: ${docId}`)
}

export function assertPathInside(parent: string, target: string): void {
    const resolvedParent = path.resolve(parent)
    const resolvedTarget = path.resolve(target)
    const relative = path.relative(resolvedParent, resolvedTarget)

    if (relative.startsWith('..') || path.isAbsolute(relative)) {
        throw new Error(`Unsafe document path: ${target}`)
    }
}
