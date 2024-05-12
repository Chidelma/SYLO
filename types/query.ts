interface _operand {
    $eq?: any
    $ne?: any
    $gt?: number
    $lt?: number
    $gte?: number
    $lte?: number
    $like?: string
}

export interface _op {
    [key: string]: _operand
}

interface _sort {
    [key: string]: 'asc' | 'desc'
}

export interface _storeQuery {
    and?: _op
    or?: _op[]
    nor?: _op[]
    limit?: number
    sort?: _sort
}