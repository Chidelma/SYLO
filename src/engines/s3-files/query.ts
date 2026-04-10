import { Cipher } from '../../adapters/cipher'
import type { CollectionIndexCache, FyloRecord, S3FilesQueryResult } from './types'

type QueryContext = {
    loadIndexCache: (collection: string) => Promise<CollectionIndexCache>
    normalizeIndexValue: (rawValue: string) => {
        rawValue: string
        valueHash: string
        valueType: string
        numericValue: number | null
    }
}

export class S3FilesQueryEngine {
    constructor(private readonly context: QueryContext) {}

    getValueByPath(target: Record<string, any>, fieldPath: string) {
        return fieldPath
            .replaceAll('/', '.')
            .split('.')
            .reduce<any>(
                (acc, key) => (acc === undefined || acc === null ? undefined : acc[key]),
                target
            )
    }

    normalizeFieldPath(fieldPath: string) {
        return fieldPath.replaceAll('.', '/')
    }

    matchesTimestamp(docId: _ttid, query?: _storeQuery<Record<string, any>>) {
        if (!query?.$created && !query?.$updated) return true
        const { createdAt, updatedAt } = TTID.decodeTime(docId)
        const timestamps = { createdAt, updatedAt: updatedAt ?? createdAt }

        const match = (value: number, range?: _timestamp) => {
            if (!range) return true
            if (range.$gt !== undefined && !(value > range.$gt)) return false
            if (range.$gte !== undefined && !(value >= range.$gte)) return false
            if (range.$lt !== undefined && !(value < range.$lt)) return false
            if (range.$lte !== undefined && !(value <= range.$lte)) return false
            return true
        }

        return (
            match(timestamps.createdAt, query.$created) &&
            match(timestamps.updatedAt, query.$updated)
        )
    }

    likeToRegex(pattern: string) {
        const escaped = pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replaceAll('%', '.*')
        return new RegExp(`^${escaped}$`)
    }

    matchesOperand(value: unknown, operand: _operand) {
        if (operand.$eq !== undefined && value != operand.$eq) return false
        if (operand.$ne !== undefined && value == operand.$ne) return false
        if (operand.$gt !== undefined && !(Number(value) > operand.$gt)) return false
        if (operand.$gte !== undefined && !(Number(value) >= operand.$gte)) return false
        if (operand.$lt !== undefined && !(Number(value) < operand.$lt)) return false
        if (operand.$lte !== undefined && !(Number(value) <= operand.$lte)) return false
        if (
            operand.$like !== undefined &&
            (typeof value !== 'string' || !this.likeToRegex(operand.$like).test(value))
        )
            return false
        if (operand.$contains !== undefined) {
            if (!Array.isArray(value) || !value.some((item) => item == operand.$contains))
                return false
        }
        return true
    }

    async normalizeQueryValue(collection: string, fieldPath: string, value: unknown) {
        let rawValue = String(value).replaceAll('/', '%2F')
        if (Cipher.isConfigured() && Cipher.isEncryptedField(collection, fieldPath))
            rawValue = await Cipher.encrypt(rawValue, true)
        return this.context.normalizeIndexValue(rawValue)
    }

    intersectDocIds(current: Set<_ttid> | null, next: Iterable<_ttid>) {
        const nextSet = next instanceof Set ? next : new Set(next)
        if (current === null) return new Set(nextSet)

        const intersection = new Set<_ttid>()
        for (const docId of current) {
            if (nextSet.has(docId)) intersection.add(docId)
        }
        return intersection
    }

    async candidateDocIdsForOperand(
        collection: string,
        fieldPath: string,
        operand: _operand
    ): Promise<Set<_ttid> | null> {
        if (Cipher.isConfigured() && Cipher.isEncryptedField(collection, fieldPath)) {
            const unsupported =
                operand.$ne !== undefined ||
                operand.$gt !== undefined ||
                operand.$gte !== undefined ||
                operand.$lt !== undefined ||
                operand.$lte !== undefined ||
                operand.$like !== undefined ||
                operand.$contains !== undefined

            if (unsupported) {
                throw new Error(`Operator is not supported on encrypted field: ${fieldPath}`)
            }
        }

        const cache = await this.context.loadIndexCache(collection)
        let candidateIds: Set<_ttid> | null = null

        if (operand.$eq !== undefined) {
            const normalized = await this.normalizeQueryValue(collection, fieldPath, operand.$eq)
            candidateIds = this.intersectDocIds(
                candidateIds,
                cache.fieldHash.get(fieldPath)?.get(normalized.valueHash) ?? new Set<_ttid>()
            )
        }

        if (
            operand.$gt !== undefined ||
            operand.$gte !== undefined ||
            operand.$lt !== undefined ||
            operand.$lte !== undefined
        ) {
            const numericMatches = new Set<_ttid>()
            for (const entry of cache.fieldNumeric.get(fieldPath) ?? []) {
                if (operand.$gt !== undefined && !(entry.numericValue > operand.$gt)) continue
                if (operand.$gte !== undefined && !(entry.numericValue >= operand.$gte)) continue
                if (operand.$lt !== undefined && !(entry.numericValue < operand.$lt)) continue
                if (operand.$lte !== undefined && !(entry.numericValue <= operand.$lte)) continue
                numericMatches.add(entry.docId)
            }

            candidateIds = this.intersectDocIds(candidateIds, numericMatches)
        }

        if (operand.$like !== undefined) {
            const regex = this.likeToRegex(operand.$like.replaceAll('/', '%2F'))
            const stringMatches = new Set<_ttid>()
            for (const entry of cache.fieldString.get(fieldPath) ?? []) {
                if (regex.test(entry.rawValue)) stringMatches.add(entry.docId)
            }

            candidateIds = this.intersectDocIds(candidateIds, stringMatches)
        }

        if (operand.$contains !== undefined) {
            const normalized = await this.normalizeQueryValue(
                collection,
                fieldPath,
                operand.$contains
            )
            const containsMatches = new Set<_ttid>()
            for (const [candidateFieldPath, hashes] of cache.fieldHash.entries()) {
                if (
                    candidateFieldPath !== fieldPath &&
                    !candidateFieldPath.startsWith(`${fieldPath}/`)
                )
                    continue
                for (const docId of hashes.get(normalized.valueHash) ?? [])
                    containsMatches.add(docId)
            }

            candidateIds = this.intersectDocIds(candidateIds, containsMatches)
        }

        return candidateIds
    }

    async candidateDocIdsForOperation<T extends Record<string, any>>(
        collection: string,
        operation: _op<T>
    ): Promise<Set<_ttid> | null> {
        let candidateIds: Set<_ttid> | null = null

        for (const [field, operand] of Object.entries(operation) as Array<[keyof T, _operand]>) {
            if (!operand) continue

            const fieldPath = this.normalizeFieldPath(String(field))
            const fieldCandidates = await this.candidateDocIdsForOperand(
                collection,
                fieldPath,
                operand
            )

            if (fieldCandidates === null) continue
            candidateIds = this.intersectDocIds(candidateIds, fieldCandidates)
        }

        return candidateIds
    }

    async candidateDocIdsForQuery<T extends Record<string, any>>(
        collection: string,
        query?: _storeQuery<T>
    ): Promise<Set<_ttid> | null> {
        if (!query?.$ops || query.$ops.length === 0) return null

        const union = new Set<_ttid>()
        let usedIndex = false

        for (const operation of query.$ops) {
            const candidateIds = await this.candidateDocIdsForOperation(collection, operation)
            if (candidateIds === null) return null
            usedIndex = true
            for (const docId of candidateIds) union.add(docId)
        }

        return usedIndex ? union : null
    }

    matchesQuery<T extends Record<string, any>>(docId: _ttid, doc: T, query?: _storeQuery<T>) {
        if (!this.matchesTimestamp(docId, query as _storeQuery<Record<string, any>> | undefined))
            return false
        if (!query?.$ops || query.$ops.length === 0) return true

        return query.$ops.some((operation) => {
            for (const field in operation) {
                const value = this.getValueByPath(doc, field)
                if (!this.matchesOperand(value, operation[field as keyof T]!)) return false
            }
            return true
        })
    }

    selectValues<T extends Record<string, any>>(selection: Array<keyof T>, data: T) {
        const copy = { ...data }
        for (const field in copy) {
            if (!selection.includes(field as keyof T)) delete copy[field]
        }
        return copy
    }

    renameFields<T extends Record<string, any>>(rename: Record<keyof Partial<T>, string>, data: T) {
        const copy = { ...data }
        for (const field in copy) {
            if (rename[field]) {
                copy[rename[field]] = copy[field]
                delete copy[field]
            }
        }
        return copy
    }

    processDoc<T extends Record<string, any>>(
        doc: FyloRecord<T>,
        query?: _storeQuery<T>
    ): S3FilesQueryResult<T> | undefined {
        if (Object.keys(doc).length === 0) return

        const next = { ...doc }

        for (let [_id, data] of Object.entries(next)) {
            if (query?.$select?.length)
                data = this.selectValues(query.$select as Array<keyof T>, data)
            if (query?.$rename) data = this.renameFields(query.$rename, data)
            next[_id as _ttid] = data as T
        }

        if (query?.$groupby) {
            const docGroup: Record<string, Record<string, Partial<T>>> = {}
            for (const [id, data] of Object.entries(next)) {
                const groupValue = data[query.$groupby] as string
                if (groupValue) {
                    const groupData = { ...data }
                    delete groupData[query.$groupby]
                    docGroup[groupValue] = { [id]: groupData as Partial<T> }
                }
            }

            if (query.$onlyIds) {
                const groupedIds: Record<string, _ttid[]> = {}
                for (const group in docGroup)
                    groupedIds[group] = Object.keys(docGroup[group]) as _ttid[]
                return groupedIds
            }

            return docGroup
        }

        if (query?.$onlyIds) return Object.keys(next).shift() as _ttid

        return next
    }
}
