/**
 * AES-256-CBC encryption adapter for field-level value encryption.
 *
 * Two modes are supported via the `deterministic` flag on `encrypt()`:
 *
 * - **Random IV (default)**: A cryptographically random IV is generated per
 *   encryption operation. Identical plaintexts produce different ciphertexts.
 *   Use this for fields that do not need exact-match ($eq/$ne) queries.
 *
 * - **Deterministic IV (opt-in)**: The IV is derived from HMAC-SHA256 of the
 *   plaintext, so identical values always produce identical ciphertext. This
 *   enables exact-match queries on encrypted fields but leaks equality — an
 *   observer can determine which records share field values without decrypting.
 *   Use only when $eq/$ne queries on encrypted fields are required.
 *
 * Encrypted fields are declared per-collection in JSON schema files via the
 * `$encrypted` array. The encryption key is sourced from `ENCRYPTION_KEY` env var.
 * Set `CIPHER_SALT` to a unique random value to prevent cross-deployment attacks.
 */

export class Cipher {
    private static key: CryptoKey | null = null
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

        // Derive 48 bytes: 32 for AES key + 16 for HMAC key
        const bits = await crypto.subtle.deriveBits(
            {
                name: 'PBKDF2',
                salt: encoder.encode(cipherSalt ?? 'fylo-cipher'),
                iterations: 100000,
                hash: 'SHA-256'
            },
            keyMaterial,
            384
        )

        const derived = new Uint8Array(bits)

        Cipher.key = await crypto.subtle.importKey(
            'raw',
            derived.slice(0, 32),
            { name: 'AES-CBC' },
            false,
            ['encrypt', 'decrypt']
        )

        Cipher.hmacKey = await crypto.subtle.importKey(
            'raw',
            derived.slice(32),
            { name: 'HMAC', hash: 'SHA-256' },
            false,
            ['sign']
        )
    }

    static reset(): void {
        Cipher.key = null
        Cipher.hmacKey = null
        Cipher.collections = new Map()
    }

    /**
     * Deterministic IV from HMAC-SHA256 of plaintext, truncated to 16 bytes.
     */
    private static async deriveIV(plaintext: string): Promise<Uint8Array> {
        const encoder = new TextEncoder()
        const sig = await crypto.subtle.sign('HMAC', Cipher.hmacKey!, encoder.encode(plaintext))
        return new Uint8Array(sig).slice(0, 16)
    }

    /**
     * Encrypts a value. Returns a URL-safe base64 string (no slashes).
     *
     * @param value - The plaintext to encrypt.
     * @param deterministic - When true, derives IV from HMAC of plaintext (same
     *   input always produces same ciphertext). Required for $eq/$ne queries on
     *   encrypted fields. Defaults to false (random IV per operation).
     */
    static async encrypt(value: string, deterministic = false): Promise<string> {
        if (!Cipher.key) throw new Error('Cipher not configured — set ENCRYPTION_KEY env var')

        const iv = deterministic
            ? await Cipher.deriveIV(value)
            : crypto.getRandomValues(new Uint8Array(16))
        const encoder = new TextEncoder()

        const encrypted = await crypto.subtle.encrypt(
            { name: 'AES-CBC', iv: iv as any },
            Cipher.key,
            encoder.encode(value)
        )

        // Concatenate IV + ciphertext and encode as URL-safe base64
        const combined = new Uint8Array(iv.length + encrypted.byteLength)
        combined.set(iv)
        combined.set(new Uint8Array(encrypted), iv.length)

        return btoa(String.fromCharCode(...combined))
            .replace(/\+/g, '-')
            .replace(/\//g, '_')
            .replace(/=+$/, '')
    }

    /**
     * Decrypts a URL-safe base64 encoded value back to plaintext.
     */
    static async decrypt(encoded: string): Promise<string> {
        if (!Cipher.key) throw new Error('Cipher not configured — set ENCRYPTION_KEY env var')

        // Restore standard base64
        const b64 = encoded.replace(/-/g, '+').replace(/_/g, '/')
        const padded = b64 + '='.repeat((4 - (b64.length % 4)) % 4)

        const combined = Uint8Array.from(atob(padded), (c) => c.charCodeAt(0))
        const iv = combined.slice(0, 16)
        const ciphertext = combined.slice(16)

        const decrypted = await crypto.subtle.decrypt(
            { name: 'AES-CBC', iv },
            Cipher.key,
            ciphertext
        )

        return new TextDecoder().decode(decrypted)
    }
}
