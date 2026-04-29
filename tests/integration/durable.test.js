import { afterAll, describe, expect, test, spyOn } from 'bun:test'
import { mkdtemp, open, readdir, rm } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { writeDurable } from '../../src/storage/durable.js'

const probe = await open(path.join(os.tmpdir(), 'fylo-fh-probe.tmp'), 'w')
const FileHandlePrototype = Object.getPrototypeOf(probe)
await probe.close()

const root = await mkdtemp(path.join(os.tmpdir(), 'fylo-durable-'))

describe('writeDurable', () => {
    afterAll(async () => {
        await rm(root, { recursive: true, force: true })
    })
    test('writes content to target and leaves no .tmp residue', async () => {
        const target = path.join(root, 'nested', 'a', 'file.txt')
        await writeDurable(target, 'hello world')
        expect(await Bun.file(target).text()).toBe('hello world')
        const entries = await readdir(path.dirname(target))
        expect(entries.filter((f) => f.endsWith('.tmp'))).toEqual([])
    })
    test('overwrites existing content atomically', async () => {
        const target = path.join(root, 'overwrite.txt')
        await writeDurable(target, 'one')
        await writeDurable(target, 'two')
        expect(await Bun.file(target).text()).toBe('two')
    })
    test('calls fsync on the file and on the parent directory', async () => {
        const syncSpy = spyOn(FileHandlePrototype, 'sync')
        const callCountBefore = syncSpy.mock.calls.length
        const target = path.join(root, 'fsync-check.txt')
        await writeDurable(target, 'payload')
        const callCountAfter = syncSpy.mock.calls.length
        expect(callCountAfter - callCountBefore).toBeGreaterThanOrEqual(2)
        syncSpy.mockRestore()
    })
    test('creates parent directories as needed', async () => {
        const target = path.join(root, 'deep', 'a', 'b', 'c', 'file.txt')
        await writeDurable(target, 'deep content')
        expect(await Bun.file(target).text()).toBe('deep content')
    })
})
