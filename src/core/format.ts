import TTID from '@d31ma/ttid'

export type TableAlignment = 'left' | 'center' | 'right' | 'auto'

export type FormatTableOptions = {
    emptyMessage?: string
    maxColumnWidth?: number
    maxKeyColumnWidth?: number
    terminalWidth?: number | 'auto'
    pageSize?: number
    wrap?: boolean
    cellAlign?: TableAlignment
    keyAlign?: Exclude<TableAlignment, 'auto'>
    headerAlign?: Exclude<TableAlignment, 'auto'>
}

type NormalizedRow = Record<string, unknown>

type Column = {
    key: string
    width: number
    minWidth: number
}

const DEFAULT_EMPTY_MESSAGE = '(no rows)'
const DEFAULT_MAX_COLUMN_WIDTH = 48
const DEFAULT_MAX_KEY_COLUMN_WIDTH = 72
const DEFAULT_MIN_CONTENT_WIDTH = 4

const ANSI_PATTERN = /\u001b\[[0-9;]*m/g
const COMBINING_MARK_PATTERN = /\p{Mark}/u

function isPlainRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function stripAnsi(value: string) {
    return value.replace(ANSI_PATTERN, '')
}

function isWideCodePoint(codePoint: number) {
    return (
        codePoint >= 0x1100 &&
        (codePoint <= 0x115f ||
            codePoint === 0x2329 ||
            codePoint === 0x232a ||
            (codePoint >= 0x2e80 && codePoint <= 0xa4cf && codePoint !== 0x303f) ||
            (codePoint >= 0xac00 && codePoint <= 0xd7a3) ||
            (codePoint >= 0xf900 && codePoint <= 0xfaff) ||
            (codePoint >= 0xfe10 && codePoint <= 0xfe19) ||
            (codePoint >= 0xfe30 && codePoint <= 0xfe6f) ||
            (codePoint >= 0xff00 && codePoint <= 0xff60) ||
            (codePoint >= 0xffe0 && codePoint <= 0xffe6) ||
            (codePoint >= 0x1f300 && codePoint <= 0x1faff))
    )
}

function displayWidth(value: string) {
    let width = 0

    for (const char of stripAnsi(value)) {
        if (COMBINING_MARK_PATTERN.test(char)) continue

        const codePoint = char.codePointAt(0)
        if (!codePoint || codePoint <= 0x1f || (codePoint >= 0x7f && codePoint <= 0x9f)) continue

        width += isWideCodePoint(codePoint) ? 2 : 1
    }

    return width
}

function truncateToWidth(value: string, maxWidth: number) {
    if (maxWidth <= 0) return ''
    if (displayWidth(value) <= maxWidth) return value
    if (maxWidth <= 3) return '.'.repeat(maxWidth)

    let result = ''
    let width = 0

    for (const char of value) {
        const charWidth = displayWidth(char)
        if (width + charWidth > maxWidth - 3) break
        result += char
        width += charWidth
    }

    return `${result}...`
}

function padLeft(value: string, width: number) {
    return `${value}${' '.repeat(Math.max(0, width - displayWidth(value)))}`
}

function padRight(value: string, width: number) {
    return `${' '.repeat(Math.max(0, width - displayWidth(value)))}${value}`
}

function padCenter(value: string, width: number) {
    const visibleWidth = displayWidth(value)
    const padding = Math.max(0, width - visibleWidth)
    const left = Math.floor(padding / 2)
    const right = padding - left
    return `${' '.repeat(left)}${value}${' '.repeat(right)}`
}

function flattenRow(
    value: unknown,
    path: string[] = [],
    output: Map<string, unknown> = new Map()
): Map<string, unknown> {
    if (isPlainRecord(value) && Object.keys(value).length > 0) {
        for (const [key, child] of Object.entries(value)) {
            flattenRow(child, [...path, key], output)
        }
        return output
    }

    const columnKey = path.length > 0 ? path.join('.') : 'value'
    output.set(columnKey, value)
    return output
}

function normalizeRow(value: unknown): NormalizedRow {
    return Object.fromEntries(flattenRow(value))
}

function formatValue(value: unknown) {
    if (value === null) return 'null'
    if (value === undefined) return ''
    if (typeof value === 'string') return value
    if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint')
        return String(value)

    try {
        return JSON.stringify(value)
    } catch {
        return String(value)
    }
}

function resolveTerminalWidth(terminalWidth?: number | 'auto') {
    if (typeof terminalWidth === 'number' && Number.isFinite(terminalWidth) && terminalWidth > 0)
        return Math.floor(terminalWidth)

    const stdoutWidth = process.stdout?.columns
    if (typeof stdoutWidth === 'number' && stdoutWidth > 0) return stdoutWidth

    const envWidth = Number(process.env.COLUMNS)
    if (Number.isFinite(envWidth) && envWidth > 0) return Math.floor(envWidth)

    return undefined
}

function detectRowKeyLabel(keys: string[]) {
    return keys.some((key) =>
        key
            .split(',')
            .map((part) => part.trim())
            .some((part) => TTID.isTTID(part))
    )
        ? '_id'
        : '_key'
}

function buildColumns(
    rows: Array<{ key: string; values: NormalizedRow }>,
    maxColumnWidth: number
): Column[] {
    const widths = new Map<string, number>()
    const order: string[] = []

    for (const row of rows) {
        for (const [columnKey, value] of Object.entries(row.values)) {
            if (!widths.has(columnKey)) {
                widths.set(columnKey, 0)
                order.push(columnKey)
            }

            const contentWidth = Math.max(
                displayWidth(columnKey),
                Math.min(displayWidth(formatValue(value)), maxColumnWidth)
            )
            widths.set(columnKey, Math.max(widths.get(columnKey) ?? 0, contentWidth))
        }
    }

    return order.map((columnKey) => ({
        key: columnKey,
        width: (widths.get(columnKey) ?? displayWidth(columnKey)) + 2,
        minWidth: DEFAULT_MIN_CONTENT_WIDTH + 1
    }))
}

function renderBorder(columns: Column[], left: string, middle: string, right: string) {
    return `${left}${columns.map((column) => '─'.repeat(column.width)).join(middle)}${right}`
}

function totalTableWidth(columns: Column[]) {
    return columns.reduce((sum, column) => sum + column.width, 0) + columns.length + 1
}

function fitColumnsToTerminal(columns: Column[], terminalWidth?: number) {
    if (!terminalWidth || totalTableWidth(columns) <= terminalWidth) return columns

    const fitted = columns.map((column) => ({ ...column }))

    while (totalTableWidth(fitted) > terminalWidth) {
        let candidateIndex = -1

        for (let index = 0; index < fitted.length; index++) {
            const column = fitted[index]!
            if (column.width <= column.minWidth) continue

            if (candidateIndex === -1 || column.width > fitted[candidateIndex]!.width) {
                candidateIndex = index
            }
        }

        if (candidateIndex === -1) break
        fitted[candidateIndex]!.width -= 1
    }

    return fitted
}

function wrapToWidth(value: string, maxWidth: number) {
    if (maxWidth <= 0) return ['']
    if (value.length === 0) return ['']

    const lines: string[] = []
    const paragraphs = value.split('\n')

    for (const paragraph of paragraphs) {
        if (paragraph.length === 0) {
            lines.push('')
            continue
        }

        const words = paragraph.split(/\s+/).filter(Boolean)
        let current = ''

        for (const word of words) {
            const candidate = current.length > 0 ? `${current} ${word}` : word

            if (displayWidth(candidate) <= maxWidth) {
                current = candidate
                continue
            }

            if (current.length > 0) {
                lines.push(current)
                current = ''
            }

            if (displayWidth(word) <= maxWidth) {
                current = word
                continue
            }

            let remaining = word
            while (displayWidth(remaining) > maxWidth) {
                const segment = truncateToWidth(remaining, maxWidth)
                const bareSegment = segment.endsWith('...') ? segment.slice(0, -3) : segment
                lines.push(bareSegment)
                remaining = remaining.slice(bareSegment.length)
            }
            current = remaining
        }

        lines.push(current)
    }

    return lines
}

function alignValue(value: string, width: number, alignment: Exclude<TableAlignment, 'auto'>) {
    if (alignment === 'left') return padLeft(value, width)
    if (alignment === 'right') return padRight(value, width)
    return padCenter(value, width)
}

function resolveAlignment(
    value: unknown,
    requested: TableAlignment
): Exclude<TableAlignment, 'auto'> {
    if (requested !== 'auto') return requested
    if (typeof value === 'number' || typeof value === 'bigint') return 'right'
    if (typeof value === 'boolean') return 'center'
    return 'left'
}

function formatCellLines(
    value: unknown,
    contentWidth: number,
    alignment: TableAlignment,
    wrap: boolean
) {
    const formatted = formatValue(value)
    const lines = wrap
        ? wrapToWidth(formatted, contentWidth)
        : [truncateToWidth(formatted, contentWidth)]
    const resolvedAlignment = resolveAlignment(value, alignment)
    return lines.map((line) => alignValue(line, contentWidth, resolvedAlignment))
}

function renderLogicalRow(
    cells: Array<{ value: unknown; width: number; align: TableAlignment }>,
    wrap: boolean
) {
    const renderedCells = cells.map((cell) =>
        formatCellLines(cell.value, Math.max(0, cell.width - 2), cell.align, wrap)
    )
    const rowHeight = renderedCells.reduce((max, lines) => Math.max(max, lines.length), 1)
    const lines: string[] = []

    for (let lineIndex = 0; lineIndex < rowHeight; lineIndex++) {
        const segments = renderedCells.map((cellLines, cellIndex) => {
            const contentWidth = Math.max(0, cells[cellIndex]!.width - 2)
            const content = cellLines[lineIndex] ?? ' '.repeat(contentWidth)
            return ` ${content} `
        })
        lines.push(`│${segments.join('│')}│`)
    }

    return lines
}

function chunkRows<T>(rows: T[], size?: number) {
    if (!size || size <= 0) return [rows]

    const pages: T[][] = []
    for (let index = 0; index < rows.length; index += size) {
        pages.push(rows.slice(index, index + size))
    }
    return pages
}

export function formatTable(
    docs: Record<string, unknown>,
    options: FormatTableOptions = {}
): string {
    const entries = Object.entries(docs)
    if (entries.length === 0) return options.emptyMessage ?? DEFAULT_EMPTY_MESSAGE

    const maxColumnWidth = options.maxColumnWidth ?? DEFAULT_MAX_COLUMN_WIDTH
    const maxKeyColumnWidth = options.maxKeyColumnWidth ?? DEFAULT_MAX_KEY_COLUMN_WIDTH
    const terminalWidth = resolveTerminalWidth(options.terminalWidth)
    const wrap = options.wrap ?? false
    const cellAlign = options.cellAlign ?? 'auto'
    const keyAlign = options.keyAlign ?? 'left'
    const headerAlign = options.headerAlign ?? 'center'
    const rowKeyLabel = detectRowKeyLabel(entries.map(([key]) => key))
    const rows = entries.map(([key, value]) => ({
        key,
        values: normalizeRow(value)
    }))

    const columns = buildColumns(rows, maxColumnWidth)
    const keyContentWidth = rows.reduce(
        (maxWidth, row) => Math.max(maxWidth, Math.min(displayWidth(row.key), maxKeyColumnWidth)),
        displayWidth(rowKeyLabel)
    )
    const keyColumn: Column = {
        key: rowKeyLabel,
        width: keyContentWidth + 2,
        minWidth: DEFAULT_MIN_CONTENT_WIDTH + 2
    }

    const allColumns = fitColumnsToTerminal([keyColumn, ...columns], terminalWidth)
    const dataColumns = allColumns.slice(1)
    const pages = chunkRows(rows, options.pageSize)
    const renderedPages: string[] = []

    for (const pageRows of pages) {
        const lines = [
            renderBorder(allColumns, '┌', '┬', '┐'),
            ...renderLogicalRow(
                [
                    { value: rowKeyLabel, width: allColumns[0]!.width, align: headerAlign },
                    ...dataColumns.map((column) => ({
                        value: column.key,
                        width: column.width,
                        align: headerAlign
                    }))
                ],
                wrap
            ),
            renderBorder(allColumns, '├', '┼', '┤')
        ]

        for (let index = 0; index < pageRows.length; index++) {
            const row = pageRows[index]!
            lines.push(
                ...renderLogicalRow(
                    [
                        { value: row.key, width: allColumns[0]!.width, align: keyAlign },
                        ...dataColumns.map((column) => ({
                            value: row.values[column.key],
                            width: column.width,
                            align: cellAlign
                        }))
                    ],
                    wrap
                )
            )

            if (index < pageRows.length - 1) lines.push(renderBorder(allColumns, '├', '┼', '┤'))
        }

        lines.push(renderBorder(allColumns, '└', '┴', '┘'))
        renderedPages.push(lines.join('\n'))
    }

    return renderedPages.join('\n\n')
}

export function printTable(docs: Record<string, unknown>, options: FormatTableOptions = {}) {
    console.log(formatTable(docs, options))
}
