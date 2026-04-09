import { Cipher } from '../adapters/cipher'

const ENCRYPTED_FIELD_OPS = ['$ne', '$gt', '$gte', '$lt', '$lte', '$like', '$contains'] as const

export class Query {
    static async getExprs<T extends Record<string, any>>(
        collection: string,
        query: _storeQuery<T>
    ) {
        let exprs = new Set<string>()

        if (query.$ops) {
            for (const op of query.$ops) {
                for (const column in op) {
                    const col = op[column as keyof T]!

                    const fieldPath = String(column).replaceAll('.', '/')
                    const encrypted =
                        Cipher.isConfigured() && Cipher.isEncryptedField(collection, fieldPath)

                    if (encrypted) {
                        for (const opKey of ENCRYPTED_FIELD_OPS) {
                            if (col[opKey] !== undefined) {
                                throw new Error(
                                    `Operator ${opKey} is not supported on encrypted field "${String(column)}"`
                                )
                            }
                        }
                    }

                    if (col.$eq) {
                        const val = encrypted
                            ? await Cipher.encrypt(String(col.$eq).replaceAll('/', '%2F'))
                            : col.$eq
                        exprs.add(`${column}/${val}/**/*`)
                    }
                    if (col.$ne) exprs.add(`${column}/**/*`)
                    if (col.$gt) exprs.add(`${column}/**/*`)
                    if (col.$gte) exprs.add(`${column}/**/*`)
                    if (col.$lt) exprs.add(`${column}/**/*`)
                    if (col.$lte) exprs.add(`${column}/**/*`)
                    if (col.$like) exprs.add(`${column}/${col.$like.replaceAll('%', '*')}/**/*`)
                    if (col.$contains !== undefined)
                        exprs.add(
                            `${column}/*/${String(col.$contains).split('/').join('%2F')}/**/*`
                        )
                }
            }
        } else exprs = new Set([`**/*`])

        return Array.from(exprs)
    }
}
