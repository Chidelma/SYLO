import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import { mkdtemp, rm } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import Fylo from '../../src'

const root = await mkdtemp(path.join(os.tmpdir(), 'fylo-rollback-'))
const POSTS = 'rb-post'
const fylo = new Fylo({ root })

beforeAll(async () => {
    await fylo.createCollection(POSTS)
})

afterAll(async () => {
    await rm(root, { recursive: true, force: true })
})

describe('rollback compatibility', () => {
    test('rollback is a safe no-op in filesystem-first FYLO', async () => {
        const id = await fylo.putData(POSTS, {
            title: 'Still here'
        })

        await expect(fylo.rollback()).resolves.toBeUndefined()

        const after = await fylo.getDoc(POSTS, id).once()
        expect(after[id].title).toBe('Still here')
    })
})
