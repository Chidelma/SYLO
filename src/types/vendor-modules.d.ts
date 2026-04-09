declare module '@delma/ttid' {
    export type _ttid = string | `${string}-${string}` | `${string}-${string}-${string}`

    export interface _timestamps {
        createdAt: number
        updatedAt?: number
        deletedAt?: number
    }

    export default class TTID {
        static isTTID(_id: string): Date | null
        static isUUID(_id: string): RegExpMatchArray | null
        static generate(_id?: string, del?: boolean): _ttid
        static decodeTime(_id: string): _timestamps
    }
}

declare module '@delma/chex' {
    export default class Gen {
        static generateDeclaration(json: unknown, interfaceName?: string): string
        static sanitizePropertyName(key: string): string
        static fromJsonString(jsonString: string, interfaceName?: string): string
        static fromObject(obj: unknown, interfaceName?: string): string
        static validateData<T extends Record<string, unknown>>(
            collection: string,
            data: T
        ): Promise<T>
    }
}

type _ttid = import('@delma/ttid')._ttid
type _timestamps = import('@delma/ttid')._timestamps
