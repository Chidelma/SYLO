/**
 * Pass-through Cipher mock for tests that don't need real encryption.
 * Returns values as-is (base64-encoded to match real adapter interface shape).
 */
export class CipherMock {

    private static _configured = false
    private static collections: Map<string, Set<string>> = new Map()

    static isConfigured(): boolean {
        return CipherMock._configured
    }

    static hasEncryptedFields(collection: string): boolean {
        const fields = CipherMock.collections.get(collection)
        return !!fields && fields.size > 0
    }

    static isEncryptedField(collection: string, field: string): boolean {
        const fields = CipherMock.collections.get(collection)
        if (!fields || fields.size === 0) return false

        for (const pattern of fields) {
            if (field === pattern) return true
            if (field.startsWith(`${pattern}/`)) return true
        }

        return false
    }

    static registerFields(collection: string, fields: string[]): void {
        if (fields.length > 0) {
            CipherMock.collections.set(collection, new Set(fields))
        }
    }

    static async configure(_secret: string): Promise<void> {
        CipherMock._configured = true
    }

    static reset(): void {
        CipherMock._configured = false
        CipherMock.collections = new Map()
    }

    static async encrypt(value: string): Promise<string> {
        return btoa(value).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
    }

    static async decrypt(encoded: string): Promise<string> {
        const b64 = encoded.replace(/-/g, '+').replace(/_/g, '/')
        const padded = b64 + '='.repeat((4 - b64.length % 4) % 4)
        return atob(padded)
    }
}
