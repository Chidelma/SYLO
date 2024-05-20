export type _schema<T> = {
    _id?: string
} & {
    [K in keyof Omit<T, '_id'>]: T[K]
}