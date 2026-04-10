export type FyloRecord<T extends Record<string, any>> = Record<_ttid, T>

export type S3FilesQueryResult<T extends Record<string, any>> =
    | _ttid
    | FyloRecord<T>
    | Record<string, _ttid[]>
    | Record<string, Record<_ttid, Partial<T>>>
    | Record<_ttid, Partial<T>>

export type S3FilesEvent<T extends Record<string, any>> = {
    ts: number
    action: 'insert' | 'delete'
    id: _ttid
    doc?: T
}

export type StoredDoc<T extends Record<string, any>> = {
    id: _ttid
    createdAt: number
    updatedAt: number
    data: T
}

export type StoredIndexEntry = {
    fieldPath: string
    rawValue: string
    valueHash: string
    valueType: string
    numericValue: number | null
}

export type StoredCollectionIndex = {
    version: 1
    docs: Record<_ttid, StoredIndexEntry[]>
}

export type CollectionIndexCache = {
    docs: Map<_ttid, StoredIndexEntry[]>
    fieldHash: Map<string, Map<string, Set<_ttid>>>
    fieldNumeric: Map<string, Array<{ docId: _ttid; numericValue: number }>>
    fieldString: Map<string, Array<{ docId: _ttid; rawValue: string }>>
}
