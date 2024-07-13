type _uuid = `${string}-${string}-${string}-${string}-${string}`

type _storeCursor<T> = {
    [Symbol.asyncIterator](): AsyncGenerator<Map<_uuid, T> | Map<_uuid, Partial<T>> | _uuid, void, unknown>
    next(limit?: number): Promise<Map<_uuid, T> | Map<_uuid, Partial<T>> | _uuid[]>
    onDelete(): AsyncGenerator<_uuid, void, unknown>
}

type _metadata = {
    num_keys: number
}