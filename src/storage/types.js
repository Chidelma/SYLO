/**
 * @typedef {import('../types/vendor.js').TTID} TTID
 */

/**
 * @typedef {'filesystem'} FyloStorageEngineKind
 */

/**
 * @typedef {object} StorageEngine
 * @property {(path: string) => Promise<string>} read
 * @property {(path: string, data: string) => Promise<void>} write
 * @property {(path: string) => Promise<void>} delete
 * @property {(path: string) => Promise<string[]>} list
 * @property {(path: string) => Promise<void>} mkdir
 * @property {(path: string) => Promise<void>} rmdir
 * @property {(path: string) => Promise<boolean>} exists
 */

/**
 * @typedef {object} LockManager
 * @property {(collection: string, docId: TTID, owner: string, ttlMs?: number) => Promise<boolean>} acquire
 * @property {(collection: string, docId: TTID, owner: string) => Promise<void>} release
 * @property {(collection: string, owner: string, options?: { ttlMs?: number, waitTimeoutMs?: number, onTakeover?: (info: { lockPath: string, newOwner: string, previousOwner?: string }) => void }) => Promise<void>} acquireCollectionWrite
 * @property {(collection: string, owner: string) => Promise<void>} releaseCollectionWrite
 */

/**
 * @template T
 * @typedef {object} EventBus
 * @property {(collection: string, event: T) => Promise<void>} publish
 * @property {(collection: string) => AsyncGenerator<T, void, unknown>} listen
 */

/**
 * @template {Record<string, any>} T
 * @typedef {Record<TTID, T>} FyloRecord
 */

/**
 * @template {Record<string, any>} T
 * @typedef {TTID | FyloRecord<T> | Record<string, TTID[]> | Record<string, Record<TTID, Partial<T>>> | Record<TTID, Partial<T>>} FilesystemQueryResult
 */

/**
 * @template {Record<string, any>} T
 * @typedef {object} FilesystemEvent
 * @property {number} ts
 * @property {'insert' | 'delete'} action
 * @property {TTID} id
 * @property {T=} doc
 */

/**
 * @template {Record<string, any>} T
 * @typedef {object} StoredDoc
 * @property {TTID} id
 * @property {number} createdAt
 * @property {number} updatedAt
 * @property {T} data
 */

/**
 * @typedef {object} StoredVersionMeta
 * @property {1} version
 * @property {TTID} versionId
 * @property {string} lineageId
 * @property {TTID=} previousVersionId
 * @property {number=} supersededAt
 * @property {number=} deletedAt
 */

/**
 * @typedef {object} StoredHead
 * @property {1} version
 * @property {string} lineageId
 * @property {TTID} currentVersionId
 * @property {boolean=} deleted
 * @property {number=} deletedAt
 */

/**
 * @typedef {object} StoredIndexEntry
 * @property {string} fieldPath
 * @property {string} rawValue
 * @property {string} valueHash
 * @property {string} valueType
 * @property {number | null} numericValue
 */

/**
 * @typedef {object} StoredCollectionIndex
 * @property {1} version
 * @property {Record<TTID, StoredIndexEntry[]>} docs
 */

/**
 * @typedef {object} CollectionIndexCache
 * @property {Map<TTID, StoredIndexEntry[]>} docs
 * @property {Map<string, Map<string, Set<TTID>>>} fieldHash
 * @property {Map<string, Array<{ docId: TTID; numericValue: number }>>} fieldNumeric
 * @property {Map<string, Array<{ docId: TTID; rawValue: string }>>} fieldString
 */

/**
 * @typedef {object} CollectionRebuildResult
 * @property {string} collection
 * @property {boolean} worm
 * @property {number} docsScanned
 * @property {number} indexedDocs
 * @property {number} headsRebuilt
 * @property {number} versionMetasRebuilt
 * @property {number} staleHeadsRemoved
 * @property {number} staleVersionMetasRemoved
 */

/**
 * @typedef {object} CollectionInspectResult
 * @property {string} collection
 * @property {boolean} exists
 * @property {boolean} worm
 * @property {number} docsStored
 * @property {number} indexedDocs
 * @property {number} headFiles
 * @property {number} activeHeads
 * @property {number} deletedHeads
 * @property {number} versionMetas
 */

export {}
