#!/usr/bin/env bun
import { WriteWorker } from './workers/write-worker'

if (process.env.FYLO_STORAGE_ENGINE === 's3-files') {
    throw new Error('fylo.worker is not supported in s3-files engine')
}

const worker = new WriteWorker(process.env.FYLO_WORKER_ID)

await worker.run({
    batchSize: process.env.FYLO_WORKER_BATCH_SIZE ? Number(process.env.FYLO_WORKER_BATCH_SIZE) : 1,
    blockMs: process.env.FYLO_WORKER_BLOCK_MS ? Number(process.env.FYLO_WORKER_BLOCK_MS) : 1000,
    recoverOnStart: process.env.FYLO_WORKER_RECOVER_ON_START !== 'false',
    recoverIdleMs: process.env.FYLO_WORKER_RECOVER_IDLE_MS
        ? Number(process.env.FYLO_WORKER_RECOVER_IDLE_MS)
        : 30_000,
    stopWhenIdle: process.env.FYLO_WORKER_STOP_WHEN_IDLE === 'true'
})
