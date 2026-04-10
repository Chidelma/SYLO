import { Cipher } from '../adapters/cipher'

export class Dir {
    private static readonly SLASH_ASCII = '%2F'

    static async extractKeys<T>(collection: string, _id: _ttid, data: T, parentField?: string) {
        const keys: { data: string[]; indexes: string[] } = { data: [], indexes: [] }
        const obj = { ...data } as Record<string, any>

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
                        val = await Cipher.encrypt(val, true)
                    }
                    keys.data.push(`${_id}/${newField}/${i}/${val}`)
                    keys.indexes.push(`${newField}/${i}/${val}/${_id}`)
                }
                continue
            }

            let val = String(fieldValue).replaceAll('/', this.SLASH_ASCII)
            if (Cipher.isConfigured() && Cipher.isEncryptedField(collection, newField)) {
                val = await Cipher.encrypt(val, true)
            }
            keys.data.push(`${_id}/${newField}/${val}`)
            keys.indexes.push(`${newField}/${val}/${_id}`)
        }

        return keys
    }

    static parseValue(value: string) {
        try {
            return JSON.parse(value)
        } catch {
            return value
        }
    }
}
