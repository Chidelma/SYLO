type _uuid = `${string}-${string}-${string}-${string}-${string}`

interface _storeCursor<T> {
    [Symbol.asyncIterator](): AsyncGenerator<Map<_uuid, T> | Map<_uuid, Partial<T>> | _uuid, void, unknown>
    collect(): Promise<Map<_uuid, T> | Map<_uuid, Partial<T>> | _uuid[]>
    onDelete(): AsyncGenerator<_uuid, void, unknown>
}

interface _treeItem<T> {
    field: keyof T
    type?: 'string' | 'number' | 'boolean' | 'string | null' | 'number | null',
    default?: string | number | boolean | null
    children?: _treeItem<T>[]
}