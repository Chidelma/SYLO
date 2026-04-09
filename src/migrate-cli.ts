#!/usr/bin/env bun
import { migrateLegacyS3ToS3Files } from './migrate'

const [, , ...args] = process.argv

const collections = args.filter((arg) => !arg.startsWith('--'))
const recreateCollections = !args.includes('--keep-existing')
const verify = !args.includes('--no-verify')

if (collections.length === 0) {
    throw new Error(
        'Usage: fylo.migrate <collection> [collection...] [--keep-existing] [--no-verify]'
    )
}

const summary = await migrateLegacyS3ToS3Files({
    collections,
    recreateCollections,
    verify
})

console.log(JSON.stringify(summary, null, 2))
