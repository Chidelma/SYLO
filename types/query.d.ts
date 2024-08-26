interface _joinOperand<U> {
    $eq?: keyof U
    $ne?: keyof U
    $gt?: keyof U
    $lt?: keyof U
    $gte?: keyof U
    $lte?: keyof U
}

interface _timestamp {
    $gt?: number
    $lt?: number
    $gte?: number
    $lte?: number
}

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

type _on<T, U> = Partial<Record<keyof T, _joinOperand<U>>>

type _join<T, U> = {
    $select?: Array<keyof T | keyof U>
    $leftCollection: string
    $rightCollection: string
    $mode: "inner" | "left" | "right" | "outer"
    $on: _on<T, U>
    $limit?: number
    $onlyIds?: boolean
    $groupby?: keyof T | keyof U
    $rename?: Record<keyof Partial<T> | keyof Partial<U>, string>
}

interface _storeQuery<T extends Record<string, any>> {
    $select?: Array<keyof T>
    $rename?: Record<keyof Partial<T>, string>
    $collection?: string
    $ops?: Array<_op<T>>
    $limit?: number
    $onlyIds?: boolean
    $groupby?: keyof T
    $updated?: _timestamp
    $created?: _timestamp
}

interface _condition { column: string, operator: string, value: string | number| boolean | null }

type _storeUpdate<T extends Record<string, any>> = {
    $collection?: string
    $where?: _storeQuery<T>
    $set: {
        [K in keyof Partial<T>]: T[K]
    }
}

type _storeDelete<T extends Record<string, any>> = _storeQuery<T>

type _storeInsert<T extends Record<string, any>> = {
    $collection?: string
    $values: {
        [K in keyof T]: T[K]
    }
}
