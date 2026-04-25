import { afterAll, describe, expect, test } from 'bun:test'
import { mkdtemp, rm } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import Fylo from '../../src/index.js'

const roots = []

async function createRoot(prefix) {
    const root = await mkdtemp(path.join(os.tmpdir(), prefix))
    roots.push(root)
    return root
}

async function run(args, cwd) {
    const proc = Bun.spawn(['bun', ...args], {
        cwd,
        stdout: 'pipe',
        stderr: 'pipe'
    })

    const [stdout, stderr, exitCode] = await Promise.all([
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
        proc.exited
    ])

    return { stdout, stderr, exitCode }
}

afterAll(async () => {
    await Promise.all(roots.map((root) => rm(root, { recursive: true, force: true })))
})

describe('CLI', () => {
    test('build emits a working CLI with SQL and richer admin commands', async () => {
        const repo = process.cwd()
        const root = await createRoot('fylo-cli-')

        const build = await run(['run', 'build'], repo)
        expect(build.exitCode).toBe(0)
        expect(build.stderr.toLowerCase()).not.toContain('error')

        const rebuild = await run(
            ['dist/cli/index.js', 'rebuild', 'cli-posts', '--root', root, '--json'],
            repo
        )
        expect(rebuild.exitCode).toBe(0)
        const rebuildResult = JSON.parse(rebuild.stdout)
        expect(rebuildResult.collection).toBe('cli-posts')
        expect(rebuildResult.docsScanned).toBe(0)
        expect(rebuildResult.indexedDocs).toBe(0)

        const create = await run(
            ['dist/cli/index.js', 'sql', 'CREATE TABLE cli-posts', '--root', root],
            repo
        )
        expect(create.exitCode).toBe(0)
        expect(create.stdout).toContain('Successfully created schema')

        const fylo = new Fylo({ root })
        await fylo.putData('cli-posts', { title: 'CLI' })

        const select = await run(
            ['dist/cli/index.js', 'sql', 'SELECT * FROM cli-posts', '--root', root],
            repo
        )
        expect(select.exitCode).toBe(0)
        expect(select.stdout).toContain('title')
        expect(select.stdout).toContain('CLI')

        const pagedSelect = await run(
            [
                'dist/cli/index.js',
                'sql',
                'SELECT * FROM cli-posts',
                '--root',
                root,
                '--page-size',
                '1',
                '--align',
                'left',
                '--no-pager'
            ],
            repo
        )
        expect(pagedSelect.exitCode).toBe(0)
        expect(pagedSelect.stdout).toContain('title')
        expect(pagedSelect.stdout).toContain('CLI')

        const inspect = await run(
            ['dist/cli/index.js', 'inspect', 'cli-posts', '--root', root, '--json'],
            repo
        )
        expect(inspect.exitCode).toBe(0)
        const inspectResult = JSON.parse(inspect.stdout)
        expect(inspectResult.collection).toBe('cli-posts')
        expect(inspectResult.exists).toBe(true)
        expect(inspectResult.docsStored).toBe(1)
        expect(inspectResult.indexedDocs).toBe(1)
        expect(inspectResult.worm).toBe(false)

        const wormFylo = new Fylo({
            root,
            worm: { mode: 'append-only', deletePolicy: 'tombstone' }
        })
        await wormFylo.createCollection('cli-worm')
        const originalId = await wormFylo.putData('cli-worm', { title: 'v1' })
        const updatedId = await wormFylo.patchDoc('cli-worm', {
            [originalId]: { title: 'v2' }
        })

        const inspectWorm = await run(
            ['dist/cli/index.js', 'inspect', 'cli-worm', '--root', root, '--worm', '--json'],
            repo
        )
        expect(inspectWorm.exitCode).toBe(0)
        const inspectWormResult = JSON.parse(inspectWorm.stdout)
        expect(inspectWormResult.worm).toBe(true)
        expect(inspectWormResult.docsStored).toBe(2)
        expect(inspectWormResult.indexedDocs).toBe(1)
        expect(inspectWormResult.headFiles).toBe(1)
        expect(inspectWormResult.versionMetas).toBe(2)

        const getHistorical = await run(
            [
                'dist/cli/index.js',
                'get',
                'cli-worm',
                originalId,
                '--root',
                root,
                '--worm',
                '--json'
            ],
            repo
        )
        expect(getHistorical.exitCode).toBe(0)
        const getHistoricalResult = JSON.parse(getHistorical.stdout)
        expect(getHistoricalResult[originalId].title).toBe('v1')

        const latest = await run(
            [
                'dist/cli/index.js',
                'latest',
                'cli-worm',
                originalId,
                '--root',
                root,
                '--worm',
                '--json'
            ],
            repo
        )
        expect(latest.exitCode).toBe(0)
        const latestResult = JSON.parse(latest.stdout)
        expect(latestResult[updatedId].title).toBe('v2')

        const latestIdOnly = await run(
            [
                'dist/cli/index.js',
                'latest',
                'cli-worm',
                originalId,
                '--root',
                root,
                '--worm',
                '--id-only'
            ],
            repo
        )
        expect(latestIdOnly.exitCode).toBe(0)
        expect(latestIdOnly.stdout.trim()).toBe(updatedId)

        const history = await run(
            [
                'dist/cli/index.js',
                'history',
                'cli-worm',
                originalId,
                '--root',
                root,
                '--worm',
                '--json'
            ],
            repo
        )
        expect(history.exitCode).toBe(0)
        const historyResult = JSON.parse(history.stdout)
        expect(historyResult).toHaveLength(2)
        expect(historyResult[0].id).toBe(updatedId)
        expect(historyResult[1].id).toBe(originalId)
    })
})
