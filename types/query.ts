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

export type _storeQuery<T, U extends keyof T> = Partial<Record<keyof Omit<T, U>, string | number| boolean | null | Omit<_operand, "$eq">>> & {
    $and?: _op<Omit<T, U>>
    $or?: Array<_op<Omit<T, U>>>
    $nor?: Array<_op<Omit<T, U>>>
    $limit?: number
    $sort?: Partial<Record<keyof Omit<T, U>, 'asc' | 'desc'>>,
}