#!/usr/bin/env bun
import path from 'node:path'
import Fylo from '../index.js'
import { runMachineRequestSource } from './machine.js'
import { renderTableOutput, writeCliText } from './output.js'

/**
 * @typedef {import('../types/vendor.js').TTID} TTID
 * @typedef {import('./format.js').FormatTableOptions} FormatTableOptions
 * @typedef {import('./output.js').PagerMode} PagerMode
 */

/**
 * @typedef {object} ParsedArgs
 * @property {string[]} positionals
 * @property {string | undefined} root
 * @property {boolean} worm
 * @property {boolean} json
 * @property {boolean} idOnly
 * @property {number | undefined} pageSize
 * @property {'left' | 'center' | 'right' | 'auto'} align
 * @property {PagerMode} pager
 * @property {string | undefined} request
 * @property {boolean} help
 */
function usage() {
    return [
        'Usage:',
        '  fylo.query "<SQL>"',
        '  fylo.query sql "<SQL>"',
        '  fylo.exec exec --request <json|@path|-> [--root <path>] [--worm]',
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
        '  --request <v>   Machine request payload, @file path, or - for stdin',
        '  --no-pager      Disable interactive paging even on large text output',
        '  --help          Show this message'
    ].join('\n')
}

/**
 * @param {string[]} argv
 * @returns {ParsedArgs}
 */
function parseArgs(argv) {
    const positionals = []
    let root
    let worm = false
    let json = false
    let idOnly = false
    let pageSize
    /** @type {'left' | 'center' | 'right' | 'auto'} */
    let align = 'auto'
    /** @type {PagerMode} */
    let pager = 'auto'
    let request
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
            align = /** @type {'left' | 'center' | 'right' | 'auto'} */ (value)
            index++
            continue
        }
        if (arg === '--no-pager') {
            pager = 'never'
            continue
        }
        if (arg === '--request') {
            const value = argv[index + 1]
            if (!value) throw new Error('Missing value for --request')
            request = value
            index++
            continue
        }
        if (arg === '--help' || arg === '-h') {
            help = true
            continue
        }
        positionals.push(arg)
    }
    return { positionals, root, worm, json, idOnly, pageSize, align, pager, request, help }
}

/**
 * @param {string} input
 * @returns {boolean}
 */
function isSqlCommand(input) {
    return /^(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP)\b/i.test(input.trim())
}

/**
 * @param {string} command
 * @param {unknown} result
 */
function renderSqlResult(command, result) {
    switch (command.toUpperCase()) {
        case 'CREATE':
            return 'Successfully created schema'
        case 'DROP':
            return 'Successfully dropped schema'
        case 'SELECT':
            if (typeof result === 'object' && result !== null && !Array.isArray(result))
                return renderTableOutput(result, currentTableOptions)
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

/** @type {FormatTableOptions} */
let currentTableOptions = {}
/** @type {PagerMode} */
let currentPagerMode = 'auto'

/**
 * @param {ParsedArgs} args
 */
function setTableOptions(args) {
    currentTableOptions = {
        cellAlign: args.align,
        pageSize: args.pageSize,
        terminalWidth: 'auto',
        wrap: true
    }
    currentPagerMode = args.pager
}

/**
 * @param {string} sql
 * @param {string | undefined} root
 */
async function runSql(sql, root) {
    const result = await new Fylo(root ? { root } : {}).executeSQL(sql)
    const op = sql.match(/^(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP)/i)?.[0]
    if (!op) throw new Error('Missing SQL operation')
    return renderSqlResult(op, result)
}

/**
 * @param {string | undefined} root
 * @param {boolean=} worm
 * @returns {Fylo}
 */
function createFylo(root, worm = false) {
    return new Fylo({
        ...(root ? { root } : {}),
        ...(worm ? { worm: { mode: 'append-only' } } : {})
    })
}

/**
 * @param {unknown} value
 */
function printJson(value) {
    console.log(JSON.stringify(value, null, 2))
}

/**
 * @param {string | undefined} request
 * @param {{ root?: string, worm?: boolean }} overrides
 * @returns {Promise<boolean>}
 */
async function runMachineExec(request, overrides) {
    const response = await runMachineRequestSource(request, overrides)
    process.stdout.write(`${JSON.stringify(response)}\n`)
    return response.ok
}

/**
 * @param {string} collection
 * @param {string | undefined} root
 * @param {boolean=} worm
 * @param {boolean=} json
 */
async function runInspect(collection, root, worm = false, json = false) {
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

/**
 * @param {string} collection
 * @param {TTID} docId
 * @param {string | undefined} root
 * @param {boolean=} worm
 * @param {boolean=} json
 */
async function runGet(collection, docId, root, worm = false, json = false) {
    const result = await createFylo(root, worm).getDoc(collection, docId).once()
    if (Object.keys(result).length === 0) throw new Error(`Document not found: ${docId}`)
    if (json) {
        printJson(result)
        return undefined
    }
    return renderTableOutput(result, currentTableOptions)
}

/**
 * @param {string} collection
 * @param {TTID} docId
 * @param {string | undefined} root
 * @param {boolean=} worm
 * @param {boolean=} json
 * @param {boolean=} idOnly
 * @returns {Promise<string | undefined>}
 */
async function runLatest(collection, docId, root, worm = false, json = false, idOnly = false) {
    const fylo = createFylo(root, worm)
    if (idOnly) {
        const latestId = await fylo.getLatest(collection, docId, true)
        if (!latestId) throw new Error(`No active head found for ${docId}`)
        if (json) {
            printJson({ id: latestId })
            return undefined
        }
        return String(latestId)
    }
    const result = /** @type {Record<string, any>} */ (await fylo.getLatest(collection, docId))
    if (Object.keys(result).length === 0) throw new Error(`No active head found for ${docId}`)
    if (json) {
        printJson(result)
        return undefined
    }
    return renderTableOutput(result, currentTableOptions)
}

/**
 * @param {string} collection
 * @param {TTID} docId
 * @param {string | undefined} root
 * @param {boolean=} worm
 * @param {boolean=} json
 */
async function runHistory(collection, docId, root, worm = false, json = false) {
    const history = await createFylo(root, worm).getHistory(collection, docId)
    if (json) {
        printJson(history)
        return undefined
    }
    if (history.length === 0) {
        return `No history found for ${docId}`
    }
    const blocks = []
    for (const entry of history) {
        blocks.push(
            [
                `${entry.id}${entry.isHead ? ' [head]' : ''}${entry.deleted ? ' [deleted]' : ''}`,
                `  lineage: ${entry.lineageId}`,
                `  previous: ${entry.previousVersionId ?? 'none'}`,
                `  updatedAt: ${entry.updatedAt}`,
                entry.deletedAt ? `  deletedAt: ${entry.deletedAt}` : undefined,
                renderTableOutput({ [entry.id]: entry.data }, currentTableOptions)
            ]
                .filter(Boolean)
                .join('\n')
        )
    }
    return blocks.join('\n\n')
}

/**
 * @param {string} collection
 * @param {string | undefined} root
 * @param {boolean=} worm
 * @param {boolean=} json
 */
async function runRebuild(collection, root, worm = false, json = false) {
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
/**
 * @param {ParsedArgs} args
 * @returns {Promise<void>}
 */
async function main(args) {
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
        const docId = rest[1]
        if (!collection) throw new Error('Missing collection name for get')
        if (!docId) throw new Error('Missing document id for get')
        const output = await runGet(collection, docId, args.root, args.worm, args.json)
        if (output) await writeCliText(output, { pagerMode: currentPagerMode })
        return
    }
    if (command === 'latest') {
        const collection = rest[0]
        const docId = rest[1]
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
        const docId = rest[1]
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
    if (command === 'exec') {
        const ok = await runMachineExec(args.request, { root: args.root, worm: args.worm })
        if (!ok) process.exitCode = 1
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
const cliArgs = parseArgs(process.argv.slice(2))
try {
    await main(cliArgs)
} catch (error) {
    console.error(/** @type {Error} */ (error).message)
    process.exit(1)
}
