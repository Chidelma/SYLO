import { formatTable, type FormatTableOptions } from '../core/format'

export type PagerMode = 'auto' | 'never'

export type CliProcessLike = {
    env: Record<string, string | undefined>
    stdin?: {
        isTTY?: boolean
    }
    stdout?: {
        isTTY?: boolean
        columns?: number
        rows?: number
    }
}

const DEFAULT_PAGER = 'less -FRX'
const DISABLED_PAGER_VALUES = new Set(['0', 'false', 'off', 'none', 'disabled'])

export function renderTableOutput(
    docs: Record<string, unknown>,
    tableOptions: FormatTableOptions = {}
) {
    return formatTable(docs, tableOptions)
}

export function splitCommandLine(input: string) {
    const parts: string[] = []
    let current = ''
    let quote: '"' | "'" | null = null
    let escaped = false

    for (const char of input) {
        if (escaped) {
            current += char
            escaped = false
            continue
        }

        if (char === '\\') {
            escaped = true
            continue
        }

        if (quote) {
            if (char === quote) quote = null
            else current += char
            continue
        }

        if (char === '"' || char === "'") {
            quote = char
            continue
        }

        if (/\s/.test(char)) {
            if (current.length > 0) {
                parts.push(current)
                current = ''
            }
            continue
        }

        current += char
    }

    if (current.length > 0) parts.push(current)
    return parts
}

export function resolvePagerCommand(processLike: CliProcessLike = process) {
    if (processLike.env.NO_PAGER) return undefined

    const configured = processLike.env.FYLO_PAGER?.trim()
    if (configured && DISABLED_PAGER_VALUES.has(configured.toLowerCase())) return undefined
    if (configured) return configured

    const pager = processLike.env.PAGER?.trim()
    if (pager && DISABLED_PAGER_VALUES.has(pager.toLowerCase())) return undefined
    if (pager) return pager

    return DEFAULT_PAGER
}

export function shouldUsePager(
    text: string,
    mode: PagerMode = 'auto',
    processLike: CliProcessLike = process
) {
    if (mode === 'never') return false
    if (!text.trim()) return false
    if (!processLike.stdin?.isTTY || !processLike.stdout?.isTTY) return false
    if (!resolvePagerCommand(processLike)) return false

    const rowsFromEnv = Number(processLike.env.LINES)
    const terminalRows =
        processLike.stdout.rows ??
        (Number.isFinite(rowsFromEnv) && rowsFromEnv > 0 ? Math.floor(rowsFromEnv) : 24)

    return text.split('\n').length > Math.max(terminalRows - 1, 1)
}

export async function writeCliText(
    text: string,
    options: {
        pagerMode?: PagerMode
        pagerCommand?: string
        processLike?: CliProcessLike
    } = {}
) {
    const processLike = options.processLike ?? process

    if (!shouldUsePager(text, options.pagerMode ?? 'auto', processLike)) {
        console.log(text)
        return
    }

    const pagerCommand = options.pagerCommand ?? resolvePagerCommand(processLike)
    const argv = pagerCommand ? splitCommandLine(pagerCommand) : []

    if (argv.length === 0) {
        console.log(text)
        return
    }

    try {
        const proc = Bun.spawn(argv, {
            stdin: new Blob([text]),
            stdout: 'inherit',
            stderr: 'inherit'
        })

        const exitCode = await proc.exited
        if (exitCode !== 0) console.log(text)
    } catch {
        console.log(text)
    }
}
