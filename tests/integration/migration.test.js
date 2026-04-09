import { afterAll, beforeAll, describe, expect, mock, test } from 'bun:test'
import { mkdtemp, rm } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'
mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))
const { default: Fylo, migrateLegacyS3ToS3Files } = await import('../../src')
const root = await mkdtemp(path.join(os.tmpdir(), 'fylo-migrate-'))
const legacyFylo = new Fylo({ engine: 'legacy-s3' })
const s3FilesFylo = new Fylo({ engine: 's3-files', s3FilesRoot: root })
const COLLECTION = 'migration-posts'
describe('legacy-s3 to s3-files migration', () => {
    beforeAll(async () => {
        await Fylo.createCollection(COLLECTION)
        await legacyFylo.putData(COLLECTION, { id: 1, title: 'Alpha' })
        await legacyFylo.putData(COLLECTION, { id: 2, title: 'Beta' })
    })
    afterAll(async () => {
        await rm(root, { recursive: true, force: true })
    })
    test('migrates legacy data and verifies parity', async () => {
        const summary = await migrateLegacyS3ToS3Files({
            collections: [COLLECTION],
            s3FilesRoot: root,
            verify: true
        })
        expect(summary[COLLECTION].migrated).toBe(2)
        expect(summary[COLLECTION].verified).toBe(true)
        const migrated = await s3FilesFylo.executeSQL(`SELECT * FROM ${COLLECTION}`)
        expect(
            Object.values(migrated)
                .map((item) => item.title)
                .sort()
        ).toEqual(['Alpha', 'Beta'])
    })
})
