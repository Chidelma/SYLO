interface _operand {
    $eq?: any
    $ne?: any
    $gt?: number
    $lt?: number
    $gte?: number
    $lte?: number
    $like?: string
}

export type _op<T> = Partial<Record<keyof T, _operand>>

export type _storeQuery<T> = Partial<Record<keyof T, string | number | boolean | null>> & {
    $and?: _op<T>
    $or?: Array<_op<T>>
    $nor?: Array<_op<T>>
    $limit?: number
    $sort?: Partial<Record<keyof T, 'asc' | 'desc'>>,
}