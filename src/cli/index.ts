#!/usr/bin/env bun
/// <reference path="../types/index.d.ts" />
import path from 'node:path'
import Fylo from '../index'
import type { FormatTableOptions, TableAlignment } from '../core/format'
import { renderTableOutput, writeCliText, type PagerMode } from './output'

type ParsedArgs = {
    positionals: string[]
    root?: string
    worm: boolean
    json: boolean
    idOnly: boolean
    pageSize?: number
    align: TableAlignment
    pager: PagerMode
    help: boolean
}

function usage() {
    return [
        'Usage:',
        '  fylo.query "<SQL>"',
        '  fylo.query sql "<SQL>"',
        '  fylo.query inspect <collection> [--root <path>] [--worm] [--json]',
        '  fylo.query get <collection> <doc-id> [--root <path>] [--worm] [--json]',
        '  fylo.query latest <collection> <doc-or-lineage-id> [--root <path>] [--worm] [--json] [--id-only]',
        '  fylo.query history <collection> <doc-or-lineage-id> [--root <path>] [--worm] [--json]',
        '  fylo.query rebuild <collection> [--root <path>] [--worm] [--json]',
        '  fylo.admin inspect <collection> [--root <path>] [--worm] [--json]',
        '  fylo.admin get <collection> <doc-id> [--root <path>] [--worm] [--json]',
        '  fylo.admin latest <collection> <doc-or-lineage-id> [--root <path>] [--worm] [--json] [--id-only]',
        '  fylo.admin history <collection> <doc-or-lineage-id> [--root <path>] [--worm] [--json]',
        '  fylo.admin rebuild <collection> [--root <path>] [--worm] [--json]',
        '',
        'Options:',
        '  --root <path>   Override FYLO_ROOT for this command',
        '  --worm          Enable WORM-aware admin behavior for this command',
        '  --json          Emit machine-readable JSON output',
        '  --id-only       Return only the resolved document id for latest',
        '  --page-size <n> Repeat headers every n rows in text output',
        '  --align <mode>  Cell alignment: left, center, right, or auto',
        '  --no-pager      Disable interactive paging even on large text output',
        '  --help          Show this message'
    ].join('\n')
}

function parseArgs(argv: string[]): ParsedArgs {
    const positionals: string[] = []
    let root: string | undefined
    let worm = false
    let json = false
    let idOnly = false
    let pageSize: number | undefined
    let align: TableAlignment = 'auto'
    let pager: PagerMode = 'auto'
    let help = false

    for (let index = 0; index < argv.length; index++) {
        const arg = argv[index]
        if (arg === '--root') {
            const value = argv[index + 1]
            if (!value) throw new Error('Missing value for --root')
            root = path.resolve(value)
            index++
            continue
        }
        if (arg === '--json') {
            json = true
            continue
        }
        if (arg === '--worm') {
            worm = true
            continue
        }
        if (arg === '--id-only') {
            idOnly = true
            continue
        }
        if (arg === '--page-size') {
            const value = Number(argv[index + 1])
            if (!Number.isInteger(value) || value <= 0)
                throw new Error('Missing or invalid value for --page-size')
            pageSize = value
            index++
            continue
        }
        if (arg === '--align') {
            const value = argv[index + 1]
            if (!value || !['left', 'center', 'right', 'auto'].includes(value))
                throw new Error('Missing or invalid value for --align')
            align = value as TableAlignment
            index++
            continue
        }
        if (arg === '--no-pager') {
            pager = 'never'
            continue
        }
        if (arg === '--help' || arg === '-h') {
            help = true
            continue
        }
        positionals.push(arg)
    }

    return { positionals, root, worm, json, idOnly, pageSize, align, pager, help }
}

function isSqlCommand(input: string) {
    return /^(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP)\b/i.test(input.trim())
}

function renderSqlResult(command: string, result: unknown) {
    switch (command.toUpperCase()) {
        case 'CREATE':
            return 'Successfully created schema'
        case 'DROP':
            return 'Successfully dropped schema'
        case 'SELECT':
            if (typeof result === 'object' && result !== null && !Array.isArray(result))
                return renderTableOutput(result as Record<string, unknown>, currentTableOptions)
            return String(result)
        case 'INSERT':
            return String(result)
        case 'UPDATE':
            return `Successfully updated ${result} document(s)`
        case 'DELETE':
            return `Successfully deleted ${result} document(s)`
        default:
            throw new Error(`Invalid SQL operation: ${command}`)
    }
}

let currentTableOptions: FormatTableOptions = {}
let currentPagerMode: PagerMode = 'auto'

function setTableOptions(args: ParsedArgs) {
    currentTableOptions = {
        cellAlign: args.align,
        pageSize: args.pageSize,
        terminalWidth: 'auto',
        wrap: true
    }
    currentPagerMode = args.pager
}

async function runSql(sql: string, root?: string) {
    const result = await new Fylo(root ? { root } : {}).executeSQL(sql)
    const op = sql.match(/^(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP)/i)?.[0]
    if (!op) throw new Error('Missing SQL operation')
    return renderSqlResult(op, result)
}

function createFylo(root?: string, worm: boolean = false) {
    return new Fylo({
        ...(root ? { root } : {}),
        ...(worm ? { worm: { mode: 'append-only' as const } } : {})
    })
}

function printJson(value: unknown) {
    console.log(JSON.stringify(value, null, 2))
}

async function runInspect(
    collection: string,
    root?: string,
    worm: boolean = false,
    json: boolean = false
) {
    const result = await createFylo(root, worm).inspectCollection(collection)
    if (json) {
        printJson(result)
        return undefined
    }

    return [
        `Collection ${result.collection}`,
        `Exists: ${result.exists ? 'yes' : 'no'}`,
        `WORM mode: ${result.worm ? 'enabled' : 'disabled'}`,
        `Stored documents: ${result.docsStored}`,
        `Indexed documents: ${result.indexedDocs}`,
        `Head files: ${result.headFiles}`,
        `Active heads: ${result.activeHeads}`,
        `Deleted heads: ${result.deletedHeads}`,
        `Version metadata files: ${result.versionMetas}`
    ].join('\n')
}

async function runGet(
    collection: string,
    docId: _ttid,
    root?: string,
    worm: boolean = false,
    json: boolean = false
) {
    const result = await createFylo(root, worm).getDoc(collection, docId).once()
    if (Object.keys(result).length === 0) throw new Error(`Document not found: ${docId}`)

    if (json) {
        printJson(result)
        return undefined
    }

    return renderTableOutput(result as Record<string, unknown>, currentTableOptions)
}

async function runLatest(
    collection: string,
    docId: _ttid,
    root?: string,
    worm: boolean = false,
    json: boolean = false,
    idOnly: boolean = false
) {
    const fylo = createFylo(root, worm)

    if (idOnly) {
        const latestId = await fylo.getLatest(collection, docId, true)
        if (!latestId) throw new Error(`No active head found for ${docId}`)

        if (json) {
            printJson({ id: latestId })
            return undefined
        }

        return latestId
    }

    const result = await fylo.getLatest(collection, docId)
    if (Object.keys(result).length === 0) throw new Error(`No active head found for ${docId}`)

    if (json) {
        printJson(result)
        return undefined
    }

    return renderTableOutput(result as Record<string, unknown>, currentTableOptions)
}

async function runHistory(
    collection: string,
    docId: _ttid,
    root?: string,
    worm: boolean = false,
    json: boolean = false
) {
    const history = await createFylo(root, worm).getHistory(collection, docId)
    if (json) {
        printJson(history)
        return undefined
    }

    if (history.length === 0) {
        return `No history found for ${docId}`
    }

    const blocks: string[] = []
    for (const entry of history) {
        blocks.push(
            [
                `${entry.id}${entry.isHead ? ' [head]' : ''}${entry.deleted ? ' [deleted]' : ''}`,
                `  lineage: ${entry.lineageId}`,
                `  previous: ${entry.previousVersionId ?? 'none'}`,
                `  updatedAt: ${entry.updatedAt}`,
                entry.deletedAt ? `  deletedAt: ${entry.deletedAt}` : undefined,
                renderTableOutput(
                    { [entry.id]: entry.data } as Record<string, unknown>,
                    currentTableOptions
                )
            ]
                .filter(Boolean)
                .join('\n')
        )
    }

    return blocks.join('\n\n')
}

async function runRebuild(
    collection: string,
    root?: string,
    worm: boolean = false,
    json: boolean = false
) {
    const result = await createFylo(root, worm).rebuildCollection(collection)
    if (json) {
        printJson(result)
        return undefined
    }

    return [
        `Rebuilt collection ${result.collection}`,
        `WORM mode: ${result.worm ? 'enabled' : 'disabled'}`,
        `Documents scanned: ${result.docsScanned}`,
        `Indexed documents: ${result.indexedDocs}`,
        result.worm ? `Heads rebuilt: ${result.headsRebuilt}` : undefined,
        result.worm ? `Version metadata rebuilt: ${result.versionMetasRebuilt}` : undefined,
        result.worm ? `Stale heads removed: ${result.staleHeadsRemoved}` : undefined,
        result.worm
            ? `Stale version metadata removed: ${result.staleVersionMetasRemoved}`
            : undefined
    ]
        .filter(Boolean)
        .join('\n')
}

async function main() {
    const args = parseArgs(process.argv.slice(2))
    setTableOptions(args)

    if (args.help || args.positionals.length === 0) {
        console.log(usage())
        return
    }

    const [command, ...rest] = args.positionals

    if (command === 'inspect') {
        const collection = rest[0]
        if (!collection) throw new Error('Missing collection name for inspect')
        const output = await runInspect(collection, args.root, args.worm, args.json)
        if (output) await writeCliText(output, { pagerMode: currentPagerMode })
        return
    }

    if (command === 'get') {
        const collection = rest[0]
        const docId = rest[1] as _ttid | undefined
        if (!collection) throw new Error('Missing collection name for get')
        if (!docId) throw new Error('Missing document id for get')
        const output = await runGet(collection, docId, args.root, args.worm, args.json)
        if (output) await writeCliText(output, { pagerMode: currentPagerMode })
        return
    }

    if (command === 'latest') {
        const collection = rest[0]
        const docId = rest[1] as _ttid | undefined
        if (!collection) throw new Error('Missing collection name for latest')
        if (!docId) throw new Error('Missing document id for latest')
        const output = await runLatest(
            collection,
            docId,
            args.root,
            args.worm,
            args.json,
            args.idOnly
        )
        if (output) await writeCliText(output, { pagerMode: currentPagerMode })
        return
    }

    if (command === 'history') {
        const collection = rest[0]
        const docId = rest[1] as _ttid | undefined
        if (!collection) throw new Error('Missing collection name for history')
        if (!docId) throw new Error('Missing document id for history')
        const output = await runHistory(collection, docId, args.root, args.worm, args.json)
        if (output) await writeCliText(output, { pagerMode: currentPagerMode })
        return
    }

    if (command === 'rebuild') {
        const collection = rest[0]
        if (!collection) throw new Error('Missing collection name for rebuild')
        const output = await runRebuild(collection, args.root, args.worm, args.json)
        if (output) await writeCliText(output, { pagerMode: currentPagerMode })
        return
    }

    if (command === 'sql') {
        const sql = rest.join(' ').trim()
        if (!sql) throw new Error('Missing SQL statement')
        const output = await runSql(sql, args.root)
        if (output) await writeCliText(output, { pagerMode: currentPagerMode })
        return
    }

    const sql = args.positionals.join(' ').trim()
    if (!isSqlCommand(sql)) {
        console.error(usage())
        throw new Error(`Unknown command: ${command}`)
    }

    const output = await runSql(sql, args.root)
    if (output) await writeCliText(output, { pagerMode: currentPagerMode })
}

await main()
