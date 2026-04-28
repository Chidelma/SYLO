/**
 * @fileoverview Validation wrapper around `@d31ma/chex`.
 *
 * chex resolves schemas from a flat `<schemaDir>/<collection>.json` path,
 * but FYLO uses a per-collection directory layout. We bypass chex's loader
 * by pre-populating the cache it consults — chex's own `validateData` will
 * see a hit and skip its file read entirely.
 *
 * chex is also strict-key (rejects data fields not declared in the schema),
 * so FYLO-reserved metadata like `_v` is stripped before delegation. The
 * head version is re-attached on success so callers can persist `_v`.
 */
import { validateData as chexValidateData } from '@d31ma/chex'
import {
    stripVersion,
    attachVersion,
    currentVersion,
    isVersioned,
    loadHeadSchema
} from './versioning.js'

/** Cache passed into chex; we own it so chex never needs to read from disk. */
const chexCache = new Map()

/**
 * @param {string} collection
 * @param {string | null | undefined} schemaDir
 * @returns {string}
 */
function cacheKey(collection, schemaDir) {
    if (!schemaDir) return collection
    const digest = new Bun.CryptoHasher('sha256').update(schemaDir).digest('hex').slice(0, 16)
    return `${collection}__${digest}`
}

/**
 * Validate `doc` against the head schema for `collection`. Returns a doc
 * with `_v=head` attached when the collection is versioned.
 *
 * @param {string} collection
 * @param {Record<string, any>} doc
 * @param {{ schemaDir?: string|null }} [options]
 * @returns {Promise<Record<string, any>>}
 */
export async function validateAgainstHead(collection, doc, options = {}) {
    const schemaDir = options.schemaDir ?? process.env.FYLO_SCHEMA_DIR
    // Make sure chex has the head schema cached before delegating.
    const key = cacheKey(collection, schemaDir)
    if (!chexCache.has(key)) {
        const head = await loadHeadSchema(collection, schemaDir)
        if (head) chexCache.set(key, head)
    }
    const { rest } = stripVersion(doc)
    const validated = /** @type {Record<string, any>} */ (
        await chexValidateData(key, rest, { schemaDir, cache: chexCache })
    )
    if (!(await isVersioned(collection, schemaDir))) return validated
    const head = /** @type {string} */ (await currentVersion(collection, schemaDir))
    return attachVersion(validated, head)
}

/** Test/dev hook to clear the chex schema cache owned by this module. */
export function _resetValidationCache() {
    chexCache.clear()
}
