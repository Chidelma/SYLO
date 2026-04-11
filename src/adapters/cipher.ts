/**
 * AES-256-GCM encryption adapter for field-level value encryption.
 *
 * Two modes are supported via the `deterministic` flag on `encrypt()`:
 *
 * - **Random IV (default)**: A cryptographically random IV is generated per
 *   encryption operation. Identical plaintexts produce different ciphertexts.
 *   Use this for fields that do not need exact-match ($eq/$ne) queries.
 *
 * Exact-match queries use a separate keyed HMAC blind index. This leaks equality
 * and frequency for indexed values, but stored document bodies use random nonces.
 *
 * Encrypted fields are declared per-collection in JSON schema files via the
 * `$encrypted` array. The encryption key is sourced from `ENCRYPTION_KEY` env var.
 * Set `CIPHER_SALT` to a unique random value to prevent cross-deployment attacks.
 */

export class Cipher {
    private static key: CryptoKey | null = null
    private static legacyCbcKey: CryptoKey | null = null
    private static hmacKey: CryptoKey | null = null

    /** Per-collection encrypted field sets, loaded from schema `$encrypted` arrays. */
    private static collections: Map<string, Set<string>> = new Map()

    static isConfigured(): boolean {
        return Cipher.key !== null
    }

    static hasEncryptedFields(collection: string): boolean {
        const fields = Cipher.collections.get(collection)
        return !!fields && fields.size > 0
    }

    static isEncryptedField(collection: string, field: string): boolean {
        const fields = Cipher.collections.get(collection)
        if (!fields || fields.size === 0) return false

        for (const pattern of fields) {
            if (field === pattern) return true
            // Support nested: encrypting "address" encrypts "address/city" etc.
            if (field.startsWith(`${pattern}/`)) return true
        }

        return false
    }

    /**
     * Registers encrypted fields for a collection (from schema `$encrypted` array).
     */
    static registerFields(collection: string, fields: string[]): void {
        if (fields.length > 0) {
            Cipher.collections.set(collection, new Set(fields))
        }
    }

    /**
     * Derives AES + HMAC keys from a secret string. Called once at startup.
     */
    static async configure(secret: string): Promise<void> {
        const encoder = new TextEncoder()
        const keyMaterial = await crypto.subtle.importKey(
            'raw',
            encoder.encode(secret),
            'PBKDF2',
            false,
            ['deriveBits']
        )

        const cipherSalt = process.env.CIPHER_SALT
        if (!cipherSalt) {
            console.warn(
                'CIPHER_SALT is not set. Using default salt is insecure for multi-deployment use. Set CIPHER_SALT to a unique random value.'
            )
        }

        // Derive 64 bytes: 32 for AES-GCM key + 32 for HMAC blind indexes.
        const bits = await crypto.subtle.deriveBits(
            {
                name: 'PBKDF2',
                salt: encoder.encode(cipherSalt ?? 'fylo-cipher'),
                iterations: 100000,
                hash: 'SHA-256'
            },
            keyMaterial,
            512
        )

        const derived = new Uint8Array(bits)

        const key = await crypto.subtle.importKey(
            'raw',
            derived.slice(0, 32),
            { name: 'AES-GCM' },
            false,
            ['encrypt', 'decrypt']
        )

        const legacyCbcKey = await crypto.subtle.importKey(
            'raw',
            derived.slice(0, 32),
            { name: 'AES-CBC' },
            false,
            ['decrypt']
        )

        const hmacKey = await crypto.subtle.importKey(
            'raw',
            derived.slice(32, 64),
            { name: 'HMAC', hash: 'SHA-256' },
            false,
            ['sign']
        )

        Cipher.key = key
        Cipher.legacyCbcKey = legacyCbcKey
        Cipher.hmacKey = hmacKey
    }

    static reset(): void {
        Cipher.key = null
        Cipher.legacyCbcKey = null
        Cipher.hmacKey = null
        Cipher.collections = new Map()
    }

    /**
     * Deterministic nonce from HMAC-SHA256 of plaintext, truncated to 12 bytes.
     */
    private static async deriveNonce(plaintext: string): Promise<Uint8Array> {
        const encoder = new TextEncoder()
        const sig = await crypto.subtle.sign('HMAC', Cipher.hmacKey!, encoder.encode(plaintext))
        return new Uint8Array(sig).slice(0, 12)
    }

    private static base64Url(bytes: Uint8Array): string {
        let binary = ''
        for (let i = 0; i < bytes.length; i += 0x8000) {
            binary += String.fromCharCode(...bytes.slice(i, i + 0x8000))
        }

        return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
    }

    private static fromBase64Url(encoded: string): Uint8Array {
        const b64 = encoded.replace(/-/g, '+').replace(/_/g, '/')
        const padded = b64 + '='.repeat((4 - (b64.length % 4)) % 4)
        return Uint8Array.from(atob(padded), (c) => c.charCodeAt(0))
    }

    /**
     * Produces a keyed lookup token for encrypted exact-match indexes.
     */
    static async blindIndex(value: string): Promise<string> {
        if (!Cipher.hmacKey) throw new Error('Cipher not configured — set ENCRYPTION_KEY env var')

        const sig = await crypto.subtle.sign(
            'HMAC',
            Cipher.hmacKey,
            new TextEncoder().encode(value)
        )
        return `idx1.${Cipher.base64Url(new Uint8Array(sig))}`
    }

    /**
     * Encrypts a value. Returns a URL-safe base64 string (no slashes).
     *
     * @param value - The plaintext to encrypt.
     * @param deterministic - Compatibility mode for legacy deterministic callers.
     *   Prefer `blindIndex()` for query indexes and random nonces for stored data.
     */
    static async encrypt(value: string, deterministic = false): Promise<string> {
        if (!Cipher.key) throw new Error('Cipher not configured — set ENCRYPTION_KEY env var')

        const nonce = deterministic
            ? await Cipher.deriveNonce(value)
            : crypto.getRandomValues(new Uint8Array(12))
        const encoder = new TextEncoder()

        const encrypted = await crypto.subtle.encrypt(
            { name: 'AES-GCM', iv: nonce as any },
            Cipher.key,
            encoder.encode(value)
        )

        const combined = new Uint8Array(nonce.length + encrypted.byteLength)
        combined.set(nonce)
        combined.set(new Uint8Array(encrypted), nonce.length)

        return `v2.${Cipher.base64Url(combined)}`
    }

    /**
     * Decrypts a URL-safe base64 encoded value back to plaintext.
     */
    static async decrypt(encoded: string): Promise<string> {
        if (!Cipher.key) throw new Error('Cipher not configured — set ENCRYPTION_KEY env var')

        if (!encoded.startsWith('v2.')) return await Cipher.decryptLegacyCbc(encoded)

        const combined = Cipher.fromBase64Url(encoded.slice(3))
        const nonce = combined.slice(0, 12)
        const ciphertext = combined.slice(12)

        const decrypted = await crypto.subtle.decrypt(
            { name: 'AES-GCM', iv: nonce },
            Cipher.key,
            ciphertext
        )

        return new TextDecoder().decode(decrypted)
    }

    private static async decryptLegacyCbc(encoded: string): Promise<string> {
        if (!Cipher.legacyCbcKey)
            throw new Error('Cipher not configured — set ENCRYPTION_KEY env var')

        const combined = Cipher.fromBase64Url(encoded)
        const iv = combined.slice(0, 16)
        const ciphertext = combined.slice(16)

        const decrypted = await crypto.subtle.decrypt(
            { name: 'AES-CBC', iv },
            Cipher.legacyCbcKey,
            ciphertext
        )

        return new TextDecoder().decode(decrypted)
    }
}
