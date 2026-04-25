import { afterEach, beforeEach, describe, expect, test } from 'bun:test'
import { Cipher } from '../../src/security/cipher.js'

describe('Cipher.configure CIPHER_SALT requirement', () => {
    /** @type {string | undefined} */
    let previousSalt
    beforeEach(() => {
        previousSalt = process.env.CIPHER_SALT
        Cipher.reset()
    })
    afterEach(() => {
        if (previousSalt === undefined) delete process.env.CIPHER_SALT
        else process.env.CIPHER_SALT = previousSalt
        Cipher.reset()
    })

    test('throws when CIPHER_SALT is absent', async () => {
        delete process.env.CIPHER_SALT
        await expect(Cipher.configure('any-secret')).rejects.toThrow('CIPHER_SALT')
    })

    test('configures successfully when CIPHER_SALT is set', async () => {
        process.env.CIPHER_SALT = 'deadbeef'.repeat(8)
        await Cipher.configure('any-secret')
        expect(Cipher.isConfigured()).toBe(true)
    })

    test('different CIPHER_SALT values produce different blind indexes', async () => {
        process.env.CIPHER_SALT = 'aaaaaaaa'.repeat(8)
        await Cipher.configure('same-secret')
        const idxA = await Cipher.blindIndex('user@example.com')
        Cipher.reset()
        process.env.CIPHER_SALT = 'bbbbbbbb'.repeat(8)
        await Cipher.configure('same-secret')
        const idxB = await Cipher.blindIndex('user@example.com')
        expect(idxA).not.toBe(idxB)
    })
})
