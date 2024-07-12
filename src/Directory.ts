import { rmSync, existsSync } from "node:fs"
import { _metadata, _uuid } from './types/general'
import Walker from "./Walker"
import { invokeWorker } from "./utils/general"

export default class {

    static readonly DATA_PATH = process.env.DATA_PREFIX || `${process.cwd()}/db`

    private static readonly CHAR_LIMIT = 255

    private static readonly SLASH_ASCII = "%2F"

    private static walkerUrl = new URL('./workers/Walker.ts', import.meta.url).href

    static isUUID(id: string) {
        return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(id)
    }

    static async aquireLock(collection: string, id: _uuid) {

        try {

            if(await this.isLocked(collection, id)) {

                await this.queueProcess(collection, id)
    
                for await (const event of Walker.listen(`${collection}/${id}/${process.pid}`)) {
                    if(event.action === "delete") break
                }
            }
    
            await this.queueProcess(collection, id)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.aquireLock -> ${e.message}`)
        }
    }

    static async releaseLock(collection: string, id: _uuid) {

        try {

            rmSync(`${this.DATA_PATH}/${collection}/${id}/${process.pid}`, { recursive: true })

            const results = await this.searchIndexes(`${collection}/${id}/**`)

            const timeSortedDir = results.sort((a, b) => {
                const aTime = Bun.file(`${this.DATA_PATH}/${a}`).lastModified
                const bTime = Bun.file(`${this.DATA_PATH}/${b}`).lastModified
                return aTime - bTime
            })

            if(timeSortedDir.length > 0) rmSync(`${this.DATA_PATH}/${timeSortedDir[0]}`, { recursive: true })

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.releaseLock -> ${e.message}`)
        }
    }

    private static async isLocked(collection: string, id: _uuid) {

        const results = await this.searchIndexes(`${collection}/${id}/**`)

        return results.filter(p => p.split('/').length === 3).length > 0
    }

    private static async queueProcess(collection: string, id: _uuid) {

        await Bun.write(Bun.file(`${this.DATA_PATH}/${collection}/${id}/${process.pid}`, { type: JSON.stringify({ created: Date.now() }) }), '.')
    }

    static async reconstructDoc<T>(collection: string, id: _uuid) {

        const indexes = await this.searchIndexes(`${collection}/**/${id}`)
        
        const keyVals = await this.reArrangeIndexes(indexes)

        let keyVal: Record<string, string> = {}

        keyVals.forEach(data => {
            const segs = data.split('/')
            const val = segs.pop()!
            const key = segs.join('/')
            keyVal = { ...keyVal, [key]: val }
        })
        
        return this.constructDoc<T>(keyVal)
    }

    static async reArrangeIndexes(indexes: string[]) {

        const keyVals: string[] = []

        for(const index of indexes) {

            let val: string

            const file = Bun.file(`${this.DATA_PATH}/${index}`)

            const segments = index.split('/')

            const id = segments.pop()!

            if(file.size > this.CHAR_LIMIT) val = await file.text()
            else val = segments.pop()!

            const collection = segments.shift()! 

            segments.unshift(id)
            segments.unshift(collection)

            keyVals.push(`${segments.join('/')}/${val}`)
        }

        return keyVals
    }

    private static async *listen(pattern: string | string[], action: "upsert" | "delete") {

        const eventIds = new Set<string>()

        for await (const event of Walker.listen(pattern)) {

            if(event.action !== action && eventIds.has(event.id))  {
                eventIds.delete(event.id)
            } else if(event.action === action && !eventIds.has(event.id) && this.isUUID(event.id)) {
                eventIds.add(event.id)
                yield event.id as _uuid
            }
        }
    }

    static async *onDelete(pattern: string | string[]) {
        
        for await (const id of this.listen(pattern, "delete")) yield id
    }

    static async *onChange(pattern: string | string[]) {
        
        for await (const id of this.listen(pattern, "upsert")) yield id
    }

    static async searchIndexes(pattern: string | string[]) {

        const indexes: string[] = []

        if(Array.isArray(pattern)) await Promise.all(pattern.map(p => new Promise<void>(resolve => invokeWorker(this.walkerUrl, { action: 'GET', data: { pattern: p } }, resolve, indexes))))
        else indexes.push(...Walker.search(pattern))

        return indexes.flat()
    }

    static async updateIndex(index: string) {

        const segements = index.split('/')

        const collection = segements.shift()!
        const key = segements.shift()!

        const id = segements.pop()!
        const val = segements.pop()!

        const currIndexes = await this.searchIndexes(`${collection}/${key}/**/${id}`)

        currIndexes.forEach(idx => rmSync(`${this.DATA_PATH}/${idx}`, { recursive: true }))

        if(val.length > this.CHAR_LIMIT) {
            await Bun.write(Bun.file(`${this.DATA_PATH}/${collection}/${key}/${segements.join('/')}/${id}`), val)
        } else {
            await Bun.write(Bun.file(`${this.DATA_PATH}/${index}`), '.')
        }
    }

    static deleteIndex(index: string) {

        if(existsSync(`${this.DATA_PATH}/${index}`)) rmSync(`${this.DATA_PATH}/${index}`, { recursive: true })
    }

    static deconstructDoc<T>(collection: string, id: _uuid, doc: T, parentKey?: string) {

        const indexes: string[] = []

        const obj = {...doc}

        for(const key in obj) {

            const newKey = parentKey ? `${parentKey}/${key}` : key

            if(typeof obj[key] === 'object' && !Array.isArray(obj[key])) {
                indexes.push(...this.deconstructDoc(collection, id, obj[key], newKey))
            } else if(typeof obj[key] === 'object' && Array.isArray(obj[key])) {
                const items: (string | number | boolean)[] = obj[key]
                if(items.some((item) => typeof item === 'object')) throw new Error(`Cannot have an array of objects`)
                items.forEach((item, idx) => indexes.push(`${collection}/${newKey}/${idx}/${String(item).replaceAll('/', this.SLASH_ASCII)}/${id}`))
            } else indexes.push(`${collection}/${newKey}/${String(obj[key]).replaceAll('/', this.SLASH_ASCII)}/${id}`)
        }

        return indexes
    }

    static constructDoc<T>(keyVal: Record<string, string>) {

        const doc: Record<string, any> = {}

        for(let fullKey in keyVal) {

            const keys = fullKey.split('/').slice(2)

            let curr = doc

            while(keys.length > 1) {

                const key = keys.shift()!

                if(keys[0].match(/^\d+$/)) {
                    if(!Array.isArray(curr[key])) curr[key] = []
                } else {
                    if(typeof curr[key] !== 'object' || curr[key] === null) curr[key] = {}
                }

                curr = curr[key]
            }

            const lastKey = keys.shift()!

            if(lastKey.match(/^\d+$/)) curr[parseInt(lastKey, 10)] = this.parseValue(keyVal[fullKey].replaceAll(this.SLASH_ASCII, '/'))
            else curr[lastKey] = this.parseValue(keyVal[fullKey].replaceAll(this.SLASH_ASCII, '/'))
        }

        return doc as T
    }

    private static parseValue(value: string) {

        const num = Number(value) 

        if(!Number.isNaN(num)) return num

        if(value === "true") return true

        if(value === "false") return false

        if(value === 'null') return null
    
        return value
    }
}