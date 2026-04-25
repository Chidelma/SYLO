import { Cipher } from '../security/cipher.js'

/**
 * @typedef {import('../types/vendor.js').TTID} TTID
 */

export class Dir {
    static SLASH_ASCII = '%2F'

    /**
     * @param {string} collection
     * @param {TTID} _id
     * @param {Record<string, any>} data
     * @param {string=} parentField
     * @returns {Promise<{ data: string[]; indexes: string[] }>}
     */
    static async extractKeys(collection, _id, data, parentField) {
        /** @type {{ data: string[]; indexes: string[] }} */
        const keys = { data: [], indexes: [] }
        const obj = { ...data }
        for (const field in obj) {
            const newField = parentField ? `${parentField}/${field}` : field
            const fieldValue = obj[field]
            if (fieldValue && typeof fieldValue === 'object' && !Array.isArray(fieldValue)) {
                const items = await this.extractKeys(collection, _id, fieldValue, newField)
                keys.data.push(...items.data)
                keys.indexes.push(...items.indexes)
                continue
            }
            if (Array.isArray(fieldValue)) {
                if (fieldValue.some((item) => typeof item === 'object')) {
                    throw new Error(`Cannot have an array of objects`)
                }
                for (let i = 0; i < fieldValue.length; i++) {
                    let val = String(fieldValue[i]).replaceAll('/', this.SLASH_ASCII)
                    if (Cipher.isConfigured() && Cipher.isEncryptedField(collection, newField)) {
                        val = await Cipher.blindIndex(val)
                    }
                    keys.data.push(`${_id}/${newField}/${i}/${val}`)
                    keys.indexes.push(`${newField}/${i}/${val}/${_id}`)
                }
                continue
            }
            let val = String(fieldValue).replaceAll('/', this.SLASH_ASCII)
            if (Cipher.isConfigured() && Cipher.isEncryptedField(collection, newField)) {
                val = await Cipher.blindIndex(val)
            }
            keys.data.push(`${_id}/${newField}/${val}`)
            keys.indexes.push(`${newField}/${val}/${_id}`)
        }
        return keys
    }

    /**
     * @param {string} value
     * @returns {unknown}
     */
    static parseValue(value) {
        try {
            return JSON.parse(value)
        } catch {
            return value
        }
    }
}
