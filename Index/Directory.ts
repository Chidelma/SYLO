import { mkdirSync, rmSync, existsSync } from "fs"
import { glob, Glob } from "glob"
import { watch } from "chokidar"
import { _schema } from "../types/schema"
import { _keyval } from "../types/general"

export default class {

    static readonly DATA_PATH = process.env.DATA_PREFIX ?? `${process.cwd()}/db`

    private static readonly ID_KEY = "_id"

    private static readonly CHAR_LIMIT = 255

    private static readonly SLASH_ASCII = "%2F"

    static hasUUID(idx: string) {
        const segs = idx.split('/')
        return segs.length >= 5 && /[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(segs[segs.length - 2])
    }

    static async reconstructDoc<T extends _schema<T>>(collection: string, id: string) {

        const indexes = [...await this.searchIndexes(`${collection}/**/${id}/*`, true), ...await this.searchIndexes(`${collection}/**/${id}/*/`)]
        
        const keyVals = await this.reArrangeIndexes(indexes)

        let keyVal: Record<string, string> = {}

        keyVals.forEach(data => {
            const segs = data.split('/')
            const val = segs.pop()!
            const key = segs.join('/')
            keyVal = { ...keyVal, [key]: val }
        })
        
        return this.constructDoc(keyVal, id) as T
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
        
        const dataPath = this.DATA_PATH
        const queue: Record<string, Set<string>> = {}
        const hasUUID = this.hasUUID

        const isComplete = (path: string, id: string, size: number) => {

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

                if(!segs[1].includes('*')) return true

            } else {

                const segs = pattern.split('/')

                if(!segs[1].includes('*')) return true
            }

            return false
        }

        const enqueueID = (controller: ReadableStreamDefaultController, path: string) => {
            if(hasUUID(path)) {
                const segs = path.split('/')
                const size = Number(segs.pop()!)
                const id = segs.pop()!
                if(isComplete(path, id, size) || isPartialChange()) {
                    controller.enqueue(id)
                }
            }
        }

        const stream = new ReadableStream<string>({
            start(controller) {
                if(listen) {
                    watch(pattern, { cwd: dataPath }).on(action, path => {
                        enqueueID(controller, path)
                    })
                } else {
                    new Glob(pattern, { cwd: dataPath }).stream().on("data", path => {
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
                ]);
                const elapsed = Date.now() - startTime
                if(elapsed <= lowestLatency) lowestLatency = elapsed + 1
            }

            if(res.done || (data && data.limit === data.count)) break
            
            data = yield res.value as string
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

    static deconstructDoc<T extends _schema<T>>(collection: string, id: string, doc: Record<string, any>, parentKey?: string) {

        const indexes: string[] = []

        const obj = {...doc}

        delete obj[this.ID_KEY]

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

    static constructDoc(keyVal: Record<string, string>, id: string) {

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

        doc[this.ID_KEY] = id

        return doc
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