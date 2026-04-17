import { afterAll, describe, expect, test } from 'bun:test'
import { rm } from 'node:fs/promises'
import Fylo, { FyloAuthError } from '../../src'
import { createTestRoot } from '../helpers/root'

const root = await createTestRoot('fylo-auth-')

afterAll(async () => {
    await rm(root, { recursive: true, force: true })
})

describe('auth policy wrapper', () => {
    test('as() fails closed when no policy is configured', () => {
        const fylo = new Fylo({ root })

        expect(() => fylo.as({ subjectId: 'user-1' })).toThrow('auth policy is not configured')
    })

    test('authorized scoped client delegates public document operations', async () => {
        const calls = []
        const fylo = new Fylo({
            root,
            auth: {
                authorize(input) {
                    calls.push(input)
                    return input.auth.subjectId === 'user-1' && input.action !== 'doc:delete'
                }
            }
        })
        const scoped = fylo.as({ subjectId: 'user-1', tenantId: 'tenant-a', roles: ['writer'] })
        const collection = 'auth-allowed'

        await scoped.createCollection(collection)
        const id = await scoped.putData(collection, {
            tenantId: 'tenant-a',
            title: 'Allowed'
        })

        const doc = await scoped.getDoc(collection, id).once()
        expect(doc[id].title).toBe('Allowed')

        const results = {}
        for await (const value of scoped
            .findDocs(collection, {
                $ops: [{ tenantId: { $eq: 'tenant-a' } }]
            })
            .collect()) {
            Object.assign(results, value)
        }
        expect(results[id].title).toBe('Allowed')

        const nextId = await scoped.patchDoc(collection, { [id]: { title: 'Updated' } })
        expect(nextId).not.toBe(id)

        let exported = 0
        for await (const _doc of scoped.exportBulkData(collection)) exported++
        expect(exported).toBe(1)

        await expect(scoped.delDoc(collection, nextId)).rejects.toBeInstanceOf(FyloAuthError)

        expect(calls.map((call) => call.action)).toEqual(
            expect.arrayContaining([
                'collection:create',
                'doc:create',
                'doc:read',
                'doc:find',
                'doc:update',
                'bulk:export',
                'doc:delete'
            ])
        )
        expect(calls.every((call) => call.auth.subjectId === 'user-1')).toBe(true)

        await fylo.dropCollection(collection)
    })

    test('denied reads do not touch storage', async () => {
        const fylo = new Fylo({
            root,
            auth: {
                authorize(input) {
                    return input.action !== 'doc:read'
                }
            }
        })
        const collection = 'auth-denied'
        await fylo.createCollection(collection)
        const id = await fylo.putData(collection, { title: 'Private' })
        const scoped = fylo.as({ subjectId: 'blocked-user' })

        await expect(scoped.getDoc(collection, id).once()).rejects.toBeInstanceOf(FyloAuthError)

        await fylo.dropCollection(collection)
    })
})
