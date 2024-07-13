interface _operand {
    $eq?: any
    $ne?: any
    $gt?: number
    $lt?: number
    $gte?: number
    $lte?: number
    $like?: string
}

type _op<T> = Partial<Record<keyof T, _operand>>

type _storeQuery<T> = {
    $select?: Array<keyof T>
    $collection?: string
    $ops?: Array<_op<T>>
}

type _condition = { column: string, operator: string, value: string | number| boolean | null }

type _storeUpdate<T> = {
    [K in keyof Partial<T>]: T[K]
} & {
    $collection?: string
    $where?: _storeQuery<T>
}

type _storeDelete<T> = _storeQuery<T>

type _storeInsert<T> = {
    [K in keyof T]: T[K]
} & {
    $collection?: string
}