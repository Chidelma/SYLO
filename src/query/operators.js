import { Cipher } from '../security/cipher.js'

/**
 * @typedef {import('./types.js').StoreQuery<Record<string, any>>} StoreQuery
 */

/** @type {(keyof import('./types.js').Operand)[]} */
const ENCRYPTED_FIELD_OPS = ['$ne', '$gt', '$gte', '$lt', '$lte', '$like', '$contains']
export class Query {
    /**
     * Builds diagnostic prefix-index expressions that can satisfy a structured FYLO query.
     * @param {string} collection
     * @param {StoreQuery} query
     * @returns {Promise<string[]>}
     */
    static async getExprs(collection, query) {
        /** @type {Set<string>} */
        let exprs = new Set()
        if (query.$ops) {
            for (const op of query.$ops) {
                for (const column in op) {
                    /** @type {import('./types.js').Operand | undefined} */
                    const col = op[column]
                    if (!col) continue
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
                            ? await Cipher.blindIndex(String(col.$eq).replaceAll('/', '%2F'))
                            : col.$eq
                        exprs.add(`${fieldPath}/eq/${val}/**/*`)
                    }
                    if (col.$ne) exprs.add(`${fieldPath}/**/*`)
                    if (col.$gt) exprs.add(`${fieldPath}/n/**/*`)
                    if (col.$gte) exprs.add(`${fieldPath}/n/**/*`)
                    if (col.$lt) exprs.add(`${fieldPath}/nr/**/*`)
                    if (col.$lte) exprs.add(`${fieldPath}/nr/**/*`)
                    if (col.$like)
                        exprs.add(`${fieldPath}/f/${col.$like.replaceAll('%', '*')}/**/*`)
                    if (col.$contains !== undefined)
                        exprs.add(`${fieldPath}/eq/${String(col.$contains)}/**/*`)
                }
            }
        } else exprs = new Set([`**/*`])
        return Array.from(exprs)
    }
}
