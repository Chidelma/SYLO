import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import { mkdtemp, rm, stat } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import Fylo from '../../src/index.js'

const runPerf = process.env.FYLO_RUN_PERF_TESTS === 'true'

describe.skipIf(!runPerf)('filesystem engine performance', () => {
    let root = path.join(os.tmpdir(), `fylo-filesystem-perf-${Date.now()}`)
    const collection = 'filesystem-perf'
    let fylo = new Fylo({ root })

    beforeAll(async () => {
        root = await mkdtemp(root)
        fylo = new Fylo({ root })
        await fylo.createCollection(collection)
    })

    afterAll(async () => {
        await rm(root, { recursive: true, force: true })
    })

    test('keeps a single durable index file while querying a large dataset', async () => {
        const totalDocs = 2000
        const insertStart = performance.now()

        for (let index = 0; index < totalDocs; index++) {
            await fylo.putData(collection, {
                title: `doc-${index}`,
                group: index % 10,
                tags: [`tag-${index % 5}`, `batch-${Math.floor(index / 100)}`],
                meta: { score: index }
            })
        }

        const insertMs = performance.now() - insertStart

        const exactStart = performance.now()
        let exactResults = {}
        for await (const data of fylo
            .findDocs(collection, {
                $ops: [{ title: { $eq: 'doc-1555' } }]
            })
            .collect()) {
            exactResults = { ...exactResults, ...data }
        }
        const exactMs = performance.now() - exactStart

        const rangeStart = performance.now()
        let rangeCount = 0
        for await (const data of fylo
            .findDocs(collection, {
                $ops: [{ ['meta.score']: { $gte: 1900 } }]
            })
            .collect()) {
            rangeCount += Object.keys(data).length
        }
        const rangeMs = performance.now() - rangeStart

        const indexFile = path.join(root, collection, '.fylo', 'indexes', `${collection}.idx.json`)
        const indexStats = await stat(indexFile)

        expect(Object.keys(exactResults)).toHaveLength(1)
        expect(rangeCount).toBe(100)
        expect(indexStats.isFile()).toBe(true)

        console.log(
            `[FYLO perf] docs=${totalDocs} insertMs=${insertMs.toFixed(1)} exactMs=${exactMs.toFixed(
                1
            )} rangeMs=${rangeMs.toFixed(1)} indexBytes=${indexStats.size}`
        )
    }, 120_000)
})
