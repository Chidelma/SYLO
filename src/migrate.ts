import Fylo from './index'
import { S3FilesEngine } from './engines/s3-files'

type MigrateOptions = {
    collections: string[]
    s3FilesRoot?: string
    recreateCollections?: boolean
    verify?: boolean
}

function normalize<T>(value: T): T {
    if (Array.isArray(value)) return value.map((item) => normalize(item)).sort() as T
    if (value && typeof value === 'object') {
        return Object.keys(value as Record<string, unknown>)
            .sort()
            .reduce(
                (acc, key) => {
                    acc[key] = normalize((value as Record<string, unknown>)[key])
                    return acc
                },
                {} as Record<string, unknown>
            ) as T
    }
    return value
}

export async function migrateLegacyS3ToS3Files({
    collections,
    s3FilesRoot = process.env.FYLO_S3FILES_ROOT,
    recreateCollections = true,
    verify = true
}: MigrateOptions) {
    if (!s3FilesRoot) throw new Error('s3FilesRoot is required')

    const source = new Fylo({ engine: 'legacy-s3' })
    const target = new S3FilesEngine(s3FilesRoot)

    const summary: Record<string, { migrated: number; verified: boolean }> = {}

    for (const collection of collections) {
        if (recreateCollections) {
            await target.dropCollection(collection)
            await target.createCollection(collection)
        } else if (!(await target.hasCollection(collection))) {
            await target.createCollection(collection)
        }

        const docs = (await source.executeSQL<Record<string, any>>(
            `SELECT * FROM ${collection}`
        )) as Record<_ttid, Record<string, any>>

        for (const [docId, doc] of Object.entries(docs) as Array<[_ttid, Record<string, any>]>) {
            await target.putDocument(collection, docId, doc)
        }

        let verified = false

        if (verify) {
            const targetFylo = new Fylo({ engine: 's3-files', s3FilesRoot })
            const migratedDocs = (await targetFylo.executeSQL<Record<string, any>>(
                `SELECT * FROM ${collection}`
            )) as Record<_ttid, Record<string, any>>
            verified = JSON.stringify(normalize(docs)) === JSON.stringify(normalize(migratedDocs))
            if (!verified) throw new Error(`Verification failed for ${collection}`)
        }

        summary[collection] = {
            migrated: Object.keys(docs).length,
            verified
        }
    }

    return summary
}
