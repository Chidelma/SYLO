import { describe, expect, test } from 'bun:test'
import { resolvePagerCommand, shouldUsePager, splitCommandLine } from '../../src/cli/output.js'

describe('CLI pager helpers', () => {
    test('splitCommandLine preserves quoted arguments', () => {
        expect(splitCommandLine('less -FRX')).toEqual(['less', '-FRX'])
        expect(splitCommandLine('bat --pager "less -FRX"')).toEqual(['bat', '--pager', 'less -FRX'])
    })

    test('resolvePagerCommand respects env overrides and disable flags', () => {
        expect(resolvePagerCommand({ env: { FYLO_PAGER: 'most -s' } })).toBe('most -s')
        expect(resolvePagerCommand({ env: { FYLO_PAGER: 'off' } })).toBeUndefined()
        expect(resolvePagerCommand({ env: { NO_PAGER: '1' } })).toBeUndefined()
    })

    test('shouldUsePager requires tty output and enough lines', () => {
        const processLike = {
            env: {},
            stdin: { isTTY: true },
            stdout: { isTTY: true, rows: 4 }
        }

        expect(shouldUsePager('a\nb\nc\nd\ne', 'auto', processLike)).toBe(true)
        expect(shouldUsePager('a\nb', 'auto', processLike)).toBe(false)
        expect(
            shouldUsePager('a\nb\nc\nd\ne', 'auto', {
                ...processLike,
                stdout: { isTTY: false, rows: 4 }
            })
        ).toBe(false)
        expect(shouldUsePager('a\nb\nc\nd\ne', 'never', processLike)).toBe(false)
    })
})
