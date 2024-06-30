export interface _operand {
    $eq?: any
    $ne?: any
    $gt?: number
    $lt?: number
    $gte?: number
    $lte?: number
    $like?: string
}

export type _op<T> = Partial<Record<keyof T, _operand>>

export type _storeQuery<T> = {
    $select?: Array<keyof T>
    $collection?: string
    $ops?: Array<_op<Omit<T, '_id'>>>
}

export type _condition = { column: string, operator: string, value: string | number| boolean | null }

export type _storeUpdate<T> = {
    [K in keyof Partial<Omit<T, '_id'>>]: T[K]
} & {
    $collection?: string
    $where?: _storeQuery<T>
}

export type _storeDelete<T> = _storeQuery<T>

export type _storeInsert<T> = {
    [K in keyof Omit<T, '_id'>]: T[K]
} & {
    $collection?: string
}