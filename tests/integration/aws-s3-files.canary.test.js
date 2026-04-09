import { describe, expect, test } from 'bun:test'
import Fylo from '../../src'
const runCanary = process.env.FYLO_RUN_S3FILES_CANARY === 'true'
const canaryTest = runCanary ? test : test.skip
describe('aws s3-files canary', () => {
    canaryTest('mounted S3 Files root handles a real CRUD cycle', async () => {
        const collection = `canary_${Date.now()}`
        const fylo = new Fylo({
            engine: 's3-files',
            s3FilesRoot: process.env.FYLO_S3FILES_ROOT
        })
        await fylo.createCollection(collection)
        const id = await fylo.putData(collection, {
            title: 'canary',
            tags: ['aws', 's3-files']
        })
        const doc = await fylo.getDoc(collection, id).once()
        expect(doc[id].title).toBe('canary')
        await fylo.delDoc(collection, id)
        await fylo.dropCollection(collection)
    })
})
