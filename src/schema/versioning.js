/**
 * @fileoverview Schema versioning runtime.
 *
 * Each collection lives in its own directory under FYLO_SCHEMA_DIR. The head
 * schema file (whose name matches `manifest.current`) is what `@d31ma/chex`
 * validates against — FYLO pre-populates chex's cache so chex never has to
 * resolve the path itself. Frozen historical snapshots and the upgrader
 * chain live in sibling subdirectories.
 *
 * Layout:
 *   <FYLO_SCHEMA_DIR>/
 *     <collection>/
 *       manifest.json               ← { current, versions: [{v, sha256?, addedAt?}] }
 *       history/
 *         v1.json                   ← all schema versions live here (head and prior)
 *         v2.json                   ← head is whichever manifest.current points at
 *       upgraders/
 *         v1-to-v2.js               ← export default async (doc) => upgradedDoc
 *       rules.json                  ← (optional) RLS rules — see security/rules/loader.js
 *
 * Version labels are arbitrary strings ("v1", "v26.18.27-2", etc.). The
 * `manifest.versions` array is the source of truth for chain order — sorting
 * by label is not used. Labels prefixed with "v" are conventional.
 *
 * Caches are memoised per process. Long-running processes that reload
 * schemas at runtime should call `_resetCaches()` after a manifest change.
 */
import path from 'node:path'
import { pathToFileURL } from 'node:url'

export const VERSION_FIELD = '_v'

/** @type {Map<string, ManifestRecord | null>} */
const manifestCache = new Map()
/** @type {Map<string, (doc: Record<string, any>) => Promise<Record<string, any>> | Record<string, any>>} */
const upgraderCache = new Map()
/** @type {Map<string, Record<string, unknown>>} */
const headSchemaCache = new Map()

/**
 * @typedef {object} ManifestVersionEntry
 * @property {string} v
 * @property {string=} sha256
 * @property {string=} addedAt
 *
 * @typedef {object} ManifestRecord
 * @property {string} current
 * @property {ManifestVersionEntry[]} versions
 */

/** @param {string} collection @param {string} schemaDir */
function collectionDir(collection, schemaDir) {
    return path.join(schemaDir, collection)
}

/** @param {string} collection @param {string|null|undefined} schemaDir */
function cacheKey(collection, schemaDir) {
    return `${schemaDir ?? ''}:${collection}`
}

/** @param {string} collection @param {string} schemaDir */
function historyDir(collection, schemaDir) {
    return path.join(collectionDir(collection, schemaDir), 'history')
}

/** @param {string} collection @param {string} schemaDir */
function upgraderDir(collection, schemaDir) {
    return path.join(collectionDir(collection, schemaDir), 'upgraders')
}

/**
 * Path to a schema version file: `<schemaDir>/<collection>/history/<version>.json`.
 * The head version is whichever entry `manifest.current` points at — all
 * versions are siblings here, with no separate head/frozen split.
 * @param {string} collection
 * @param {string} schemaDir
 * @param {string} version
 */
function schemaVersionPath(collection, schemaDir, version) {
    return path.join(historyDir(collection, schemaDir), `${version}.json`)
}

/**
 * Load the version manifest for `collection`. Returns `null` when no manifest
 * file exists (collection isn't versioned, or doesn't have a schema).
 * @param {string} collection
 * @param {string|null|undefined} schemaDir
 * @returns {Promise<ManifestRecord|null>}
 */
export async function loadManifest(collection, schemaDir) {
    if (!schemaDir) return null
    const key = cacheKey(collection, schemaDir)
    if (manifestCache.has(key)) return manifestCache.get(key) ?? null
    const manifestPath = path.join(collectionDir(collection, schemaDir), 'manifest.json')
    const file = Bun.file(manifestPath)
    if (!(await file.exists())) {
        manifestCache.set(key, null)
        return null
    }
    const manifest = /** @type {ManifestRecord} */ (await file.json())
    if (typeof manifest.current !== 'string' || manifest.current.length === 0) {
        throw new Error(
            `Invalid manifest for '${collection}': 'current' must be a non-empty string`
        )
    }
    if (!Array.isArray(manifest.versions) || manifest.versions.length === 0) {
        throw new Error(
            `Invalid manifest for '${collection}': 'versions' must be a non-empty array`
        )
    }
    if (!manifest.versions.some((entry) => entry.v === manifest.current)) {
        throw new Error(
            `Manifest for '${collection}': 'current' (${manifest.current}) is not present in 'versions'`
        )
    }
    manifestCache.set(key, manifest)
    return manifest
}

/**
 * Head version label for `collection`. Returns `null` when the collection
 * has no manifest. Callers that need a non-null version should use the
 * three-arg `materializeDoc` / `validateAgainstHead` paths instead.
 * @param {string} collection
 * @param {string|null|undefined} schemaDir
 * @returns {Promise<string|null>}
 */
export async function currentVersion(collection, schemaDir) {
    const manifest = await loadManifest(collection, schemaDir)
    return manifest?.current ?? null
}

/**
 * Whether the collection has a manifest (and therefore a head schema).
 * @param {string} collection
 * @param {string|null|undefined} schemaDir
 * @returns {Promise<boolean>}
 */
export async function isVersioned(collection, schemaDir) {
    return (await loadManifest(collection, schemaDir)) !== null
}

/**
 * Load the head schema document (the chex regex schema). Returns `null` when
 * the collection has no manifest. Cached per process.
 * @param {string} collection
 * @param {string|null|undefined} schemaDir
 * @returns {Promise<Record<string, unknown>|null>}
 */
export async function loadHeadSchema(collection, schemaDir) {
    if (!schemaDir) return null
    const key = cacheKey(collection, schemaDir)
    const cached = headSchemaCache.get(key)
    if (cached) return cached
    const manifest = await loadManifest(collection, schemaDir)
    if (!manifest) return null
    const schemaPath = schemaVersionPath(collection, schemaDir, manifest.current)
    const file = Bun.file(schemaPath)
    if (!(await file.exists())) {
        throw new Error(
            `Manifest for '${collection}' declares head version '${manifest.current}' but ${schemaPath} does not exist`
        )
    }
    const schema = /** @type {Record<string, unknown>} */ (await file.json())
    headSchemaCache.set(key, schema)
    return schema
}

/**
 * Dynamically import an upgrader module. Cached per (collection, from→to).
 * @param {string} collection
 * @param {string} fromVersion
 * @param {string} toVersion
 * @param {string} schemaDir
 * @returns {Promise<(doc: Record<string, any>) => Promise<Record<string, any>> | Record<string, any>>}
 */
export async function loadUpgrader(collection, fromVersion, toVersion, schemaDir) {
    const key = `${schemaDir}:${collection}@${fromVersion}->${toVersion}`
    const cached = upgraderCache.get(key)
    if (cached) return cached
    const upgraderPath = path.join(
        upgraderDir(collection, schemaDir),
        `${fromVersion}-to-${toVersion}.js`
    )
    let mod
    try {
        mod = await import(pathToFileURL(upgraderPath).href)
    } catch (err) {
        const code = /** @type {NodeJS.ErrnoException} */ (err).code
        if (code === 'ERR_MODULE_NOT_FOUND' || code === 'ENOENT') {
            throw new Error(
                `Missing upgrader ${fromVersion}→${toVersion} for collection '${collection}' at ${upgraderPath}`
            )
        }
        throw err
    }
    if (typeof mod.default !== 'function') {
        throw new Error(
            `Upgrader at ${upgraderPath} must default-export an async (doc) => doc function`
        )
    }
    upgraderCache.set(key, mod.default)
    return mod.default
}

/**
 * Walk the upgrader chain from `fromVersion` to `toVersion` along the
 * manifest's `versions` array order. Each step applies one upgrader.
 *
 * @param {string} collection
 * @param {Record<string, any>} doc
 * @param {string} fromVersion
 * @param {string} toVersion
 * @param {string} schemaDir
 * @returns {Promise<Record<string, any>>}
 */
export async function upgradeDoc(collection, doc, fromVersion, toVersion, schemaDir) {
    if (fromVersion === toVersion) return doc
    const manifest = await loadManifest(collection, schemaDir)
    if (!manifest) {
        throw new Error(`No manifest for '${collection}' — cannot resolve upgrader chain`)
    }
    const order = manifest.versions.map((e) => e.v)
    const fromIdx = order.indexOf(fromVersion)
    const toIdx = order.indexOf(toVersion)
    if (fromIdx === -1) {
        throw new Error(
            `Doc in '${collection}' is at unknown version '${fromVersion}' (not in manifest.versions)`
        )
    }
    if (toIdx === -1) {
        throw new Error(`Target version '${toVersion}' is not in '${collection}' manifest.versions`)
    }
    if (fromIdx > toIdx) {
        throw new Error(
            `Doc in '${collection}' is at v${fromVersion}, ahead of target v${toVersion} — schema rolled back?`
        )
    }
    let next = doc
    for (let i = fromIdx; i < toIdx; i++) {
        const from = order[i]
        const to = order[i + 1]
        const upgrader = await loadUpgrader(collection, from, to, schemaDir)
        const result = await upgrader(next)
        if (result === null || result === undefined || typeof result !== 'object') {
            throw new Error(
                `Upgrader ${from}→${to} for '${collection}' must return an object, got ${typeof result}`
            )
        }
        next = /** @type {Record<string, any>} */ (result)
    }
    return next
}

/**
 * Pull `_v` off a doc without mutating it. Version labels are strings.
 * @param {Record<string, any> | null | undefined} doc
 * @returns {{ version: string | undefined, rest: Record<string, any> | null | undefined }}
 */
export function stripVersion(doc) {
    if (doc === null || doc === undefined || typeof doc !== 'object') {
        return { version: undefined, rest: doc }
    }
    if (!(VERSION_FIELD in doc)) return { version: undefined, rest: doc }
    const { [VERSION_FIELD]: raw, ...rest } = doc
    const version = typeof raw === 'string' ? raw : undefined
    return { version, rest }
}

/**
 * Return a new doc with `_v` set to `version`.
 * @param {Record<string, any>} doc
 * @param {string} version
 * @returns {Record<string, any>}
 */
export function attachVersion(doc, version) {
    return { ...doc, [VERSION_FIELD]: version }
}

/** Test/dev hook to reset memoised manifests, head schemas, and upgraders. */
export function _resetCaches() {
    manifestCache.clear()
    upgraderCache.clear()
    headSchemaCache.clear()
}
