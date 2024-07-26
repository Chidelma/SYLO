interface _operand {
    $eq?: any
    $ne?: any
    $gt?: number
    $lt?: number
    $gte?: number
    $lte?: number
    $like?: string
}

interface _joinOperand<U> {
    $eq?: keyof U
    $ne?: keyof U
    $gt?: keyof U
    $lt?: keyof U
    $gte?: keyof U
    $lte?: keyof U
}

type _op<T> = Partial<Record<keyof T, _operand>>

type _join<T, U> = {
    $select?: Array<keyof T | keyof U>
    $leftCollection: string
    $rightCollection: string
    $mode: "inner" | "left" | "right" | "outer"
    $limit?: number
    $onlyIds?: boolean
    $groupby?: keyof T | keyof U
    $rename?: Record<keyof Partial<T> | keyof Partial<U>, string>
} & {
    [K in keyof Partial<T>]: _joinOperand<U>
}

interface _storeQuery<T extends Record<string, any>> {
    $select?: Array<keyof T>
    $rename?: Record<keyof Partial<T>, string>
    $collection?: string
    $ops?: Array<_op<T>>
    $limit?: number
    $onlyIds?: boolean
    $groupby?: keyof T
}

interface _condition { column: string, operator: string, value: string | number| boolean | null }

type _storeUpdate<T extends Record<string, any>> = {
    [K in keyof Partial<T>]: T[K]
} & {
    $collection?: string
    $where?: _storeQuery<T>
}

type _storeDelete<T extends Record<string, any>> = _storeQuery<T>

type _storeInsert<T extends Record<string, any>> = {
    [K in keyof T]: T[K]
} & {
    $collection?: string
}
