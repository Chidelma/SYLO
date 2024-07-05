import { mkdirSync, rmSync, existsSync } from "fs"
import { glob, Glob } from "glob"
import { watch } from "chokidar"
import { _fullMerge, _uuid } from '../types/schema'

export default class {

    static readonly DATA_PATH = process.env.DATA_PREFIX || `${process.cwd()}/db`

    private static readonly CHAR_LIMIT = 255

    private static readonly SLASH_ASCII = "%2F"

    static hasUUID(idx: string) {
        const segs = idx.split('/')
        return segs.length >= 5 && /[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(segs[segs.length - 2])
    }

    static async aquireLock(collection: string, id: _uuid) {

        try {

            if(await this.isLocked(collection, id)) {

                this.queueProcess(collection, id)
    
                for await (const _ of this.onUnlock(`${collection}/${id}/${process.pid}`)) {
                    break
                }
            }
    
            this.queueProcess(collection, id)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.aquireLock -> ${e.message}`)
        }
    }

    private static async *onUnlock(pattern: string) {

        const cwd = this.DATA_PATH

        const stream = new ReadableStream<string>({
            start(controller) {
                watch(pattern, { cwd }).on("unlinkDir", path => {
                    controller.enqueue(path)
                })
            }
        })

        const reader = stream.getReader()

        const path = await reader.read()

        yield path.value
    }

    static async releaseLock(collection: string, id: _uuid) {

        try {

            rmSync(`${this.DATA_PATH}/${collection}/${id}/${process.pid}`, { recursive: true })

            const results = await glob(`${collection}/${id}/**/`, { withFileTypes: true, stat: true, cwd: this.DATA_PATH })
            
            const timeSortedDir = results.sort((a, b) => a.birthtimeMs! - b.birthtimeMs!).map(p => p.relative()).filter(p => p.split('/').length === 3)

            if(timeSortedDir.length > 0) rmSync(`${this.DATA_PATH}/${timeSortedDir[0]}`, { recursive: true })

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.releaseLock -> ${e.message}`)
        }
    }

    private static async isLocked(collection: string, id: _uuid) {

        const results = await glob(`${collection}/${id}/**/`, { cwd: this.DATA_PATH })

        return results.filter(p => p.split('/').length === 3).length > 0
    }

    private static queueProcess(collection: string, id: _uuid) {
        mkdirSync(`${this.DATA_PATH}/${collection}/${id}/${process.pid}`, { recursive: true })
    }

    static async reconstructDoc<T>(collection: string, id: _uuid) {

        const indexes = [...await this.searchIndexes(`${collection}/**/${id}/*`, true), ...await this.searchIndexes(`${collection}/**/${id}/*/`)]
        
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

            const segments = [...index.split('/')]

            segments.pop()!
            const id = segments.pop()!
            const file = Bun.file(`${this.DATA_PATH}/${index}`)
            const val = await file.exists() ? await file.text() : segments.pop()!
            const collection = segments.shift()! 

            segments.unshift(id)
            segments.unshift(collection)

            keyVals.push(`${segments.join('/')}/${val}`)
        }

        return keyVals
    }

    static async *onChange(pattern: string | string[], action: "addDir" | "unlinkDir", listen: boolean = false) {
        
        const cwd = this.DATA_PATH
        const queue: Record<string, Set<string>> = {}
        const hasUUID = this.hasUUID

        const isComplete = (path: string, id: _uuid, size: number) => {

            if(queue[id]) {

                queue[id].add(path)

                if(size === (queue[id].size + 1)) {
                    queue[id].clear()
                    return true
                }

            } else queue[id] = new Set([path])

            return false
        }

        const isPartialChange = () => {

            if(Array.isArray(pattern)) {

                const firstPattern = pattern[0]

                const segs = firstPattern.split('/')

                if(!segs.slice(1, -1).every((elem) => elem.includes('*'))) return true

            } else {

                const segs = pattern.split('/')

                if(!segs.slice(1, -1).every((elem) => elem.includes('*'))) return true
            }

            return false
        }

        const enqueueID = (controller: ReadableStreamDefaultController, path: string) => {
            if(hasUUID(path)) {
                const segs = path.split('/')
                const size = Number(segs.pop()!)
                const id = segs.pop()! as _uuid
                if(isComplete(path, id, size) || isPartialChange()) {
                    controller.enqueue(id)
                }
            }
        }

        const stream = new ReadableStream<string>({
            start(controller) {
                if(listen) {
                    watch(pattern, { cwd }).on(action, path => {
                        enqueueID(controller, path)
                    })
                } else {
                    new Glob(pattern, { cwd }).stream().on("data", path => {
                        enqueueID(controller, path)
                    })
                }
            }
        })

        const reader = stream.getReader()

        let data: { limit?: number, count: number } | undefined

        let lowestLatency = 500

        while(true) {

            let res: { done: boolean, value: string | undefined };

            if(listen) res = await reader.read() as { done: boolean, value: string | undefined }
            else {

                const startTime = Date.now()
                res = await Promise.race([
                    reader.read() as Promise<{ done: boolean, value: string | undefined }>,
                    new Promise<{ done: boolean, value: undefined }>(resolve =>
                        setTimeout(() => resolve({ done: true, value: undefined }), lowestLatency)
                    )
                ])
                const elapsed = Date.now() - startTime
                if(elapsed < lowestLatency) lowestLatency = elapsed + 1
            }

            if(res.done || (data && data.limit === data.count)) break
            
            data = yield res.value as _uuid
        }
    }

    static async searchIndexes(pattern: string | string[], nodir: boolean = false) {

        const indexes = await glob(pattern, { cwd: this.DATA_PATH, nodir })

        return indexes.filter(this.hasUUID)
    }

    static async updateIndex(index: string) {

        const segements = index.split('/')

        const collection = segements.shift()!
        const key = segements.shift()!

        const size = segements.pop()!
        const id = segements.pop()!
        const val = segements.pop()!

        const currIndexes = await this.searchIndexes(`${collection}/${key}/**/${id}/*`)

        currIndexes.forEach(idx => rmSync(`${this.DATA_PATH}/${idx}`, { recursive: true }))

        val.length > this.CHAR_LIMIT ? await Bun.write(`${this.DATA_PATH}/${collection}/${key}/${segements.join('/')}/${id}/${size}`, val) : mkdirSync(`${this.DATA_PATH}/${index}`, { recursive: true })
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