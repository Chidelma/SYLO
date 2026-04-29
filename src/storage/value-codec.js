/**
 * Escapes values that FYLO stores as plaintext document fields before encryption.
 * @param {unknown} value
 * @returns {string}
 */
export function stringifyStoredValue(value) {
    return String(value).replaceAll('/', '%2F')
}

/**
 * Parses decrypted stored values back into their closest JSON representation.
 * @param {string} value
 * @returns {unknown}
 */
export function parseStoredValue(value) {
    try {
        return JSON.parse(value)
    } catch {
        return value
    }
}
