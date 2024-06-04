import { mkdirSync, rmSync, existsSync } from "fs"
import { glob } from "glob"
import { _schema } from "../types/schema"
import { _keyval } from "../types/general"
import { watch } from "chokidar"

export default class {

    static readonly DATA_PATH = process.env.DATA_PREFIX ?? `${process.cwd()}/db`

    private static readonly uuidPattern = /[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/

    private static readonly ID_KEY = "_id"

    private static readonly CHAR_LIMIT = 255

    private static isIndex(path: string) {
        return path.split('/').length >= 4 && /[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(path)
    }

    static async reconstructDoc<T extends _schema<T>>(collection: string, id: string) {

        const fileIndexes = await this.searchIndexes(`${collection}/**/${id}`, true)
        const dirIndexes = await this.searchIndexes(`${collection}/**/${id}/`)
        
        const keyVals = await this.reArrangeIndexes([...fileIndexes, ...dirIndexes])

        let keyVal: Record<string, string> = {}

        keyVals.map(keyval => keyval.data).forEach(data => {
            const segs = data.split('/')
            const val = segs.pop()!
            const key = segs.join('/')
            keyVal = { ...keyVal, [key]: val }
        })
        
        return this.constructDoc(keyVal, id) as T
    }

    static async reArrangeIndexes(indexes: string[]) {

        const keyVals: _keyval[] = []

        for(const index of indexes) {

            const segments = [...index.split('/')]

            const id = segments.pop()!
            const val = await Bun.file(`${this.DATA_PATH}/${index}`).exists() ? await Bun.file(`${this.DATA_PATH}/${index}`).text() : segments.pop()!
            const collection = segments.shift()! 

            segments.unshift(id)
            segments.unshift(collection)

            keyVals.push({
                index,
                data: `${segments.join('/')}/${val}`
            })
        }

        return keyVals
    }

    static async *onChange(pattern: string | string[], action: "addDir" | "unlinkDir") {

        const dataPath = this.DATA_PATH
        const isIndex = this.isIndex
        const queue = new Set<string>()

        const stream = new ReadableStream<string>({
            start(controller) {
                watch(pattern, { cwd: dataPath }).on(action, path => {
                    if(isIndex(path)) {
                        const id = path.split('/').pop()!
                        if(!queue.has(id)) {
                            queue.add(id)
                            controller.enqueue(id)
                            setTimeout(() => queue.delete(id), 500)
                        }
                    }
                })
            }
        })

        const reader = stream.getReader()

        let data: { limit?: number, count: number } | undefined

        let lowestLatency = 500

        while(true) {

            let res: { done: boolean, value: string | undefined };

            if (data) {
                const startTime = Date.now()
                res = await Promise.race([
                    reader.read() as Promise<{ done: boolean, value: string | undefined }>,
                    new Promise<{ done: boolean, value: undefined }>(resolve =>
                        setTimeout(() => resolve({ done: true, value: undefined }), lowestLatency)
                    )
                ]);
                const elapsed = Date.now() - startTime
                if(elapsed <= lowestLatency) lowestLatency = elapsed
            } else {
                res = await reader.read() as { done: boolean, value: string | undefined };
            }

            if(res.done || (data && data.limit === data.count)) break
            
            data = yield res.value as string
        }
    }

    static async searchIndexes(pattern: string | string[], nodir: boolean = false) {

        const indexes = await glob(pattern, { cwd: this.DATA_PATH, nodir })

        return indexes.filter(idx => idx.split('/').length >= 4 && this.uuidPattern.test(idx))
    }

    static async updateIndex(index: string) {

        const segements = index.split('/')

        const collection = segements.shift()!
        const key = segements.shift()!
        const id = segements.pop()!
        const val = segements.pop()!

        const currIndexes = await this.searchIndexes(`${collection}/${key}/**/${id}`)

        currIndexes.forEach(idx => rmSync(`${this.DATA_PATH}/${idx}`, { recursive: true }))

        val.length > this.CHAR_LIMIT ? await Bun.write(`${this.DATA_PATH}/${collection}/${key}/${segements.join('/')}/${id}`, val) : mkdirSync(`${this.DATA_PATH}/${index}`, { recursive: true })
    }

    static deleteIndex(index: string) {

        if(existsSync(`${this.DATA_PATH}/${index}`)) rmSync(`${this.DATA_PATH}/${index}`, { recursive: true })
    }

    static deconstructDoc<T extends _schema<T>>(collection: string, id: string, doc: Record<string, any>, parentKey?: string) {

        const keyVals: _keyval[] = []

        const obj = {...doc}

        delete obj[this.ID_KEY]

        for(const key in obj) {

            const newKey = parentKey ? `${parentKey}/${key}` : key

            if(typeof obj[key] === 'object' && !Array.isArray(obj[key])) {
                keyVals.push(...this.deconstructDoc(collection, id, obj[key], newKey))
            } else if(typeof obj[key] === 'object' && Array.isArray(obj[key])) {
                const items: (string | number | boolean)[] = obj[key]
                if(items.some((item) => typeof item === 'object')) throw new Error(`Cannot have an array of objects`)
                items.forEach((item, idx) => {
                    keyVals.push({
                        data: `${collection}/${id}/${newKey}/${idx}/${item}`,
                        index: `${collection}/${newKey}/${idx}/${item}/${id}`
                    })
                })
            } else {
                keyVals.push({ 
                    data: `${collection}/${id}/${newKey}/${obj[key]}`,
                    index: `${collection}/${newKey}/${obj[key]}/${id}`
                })
            }
        }

        return keyVals
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

            if(lastKey.match(/^\d+$/)) curr[parseInt(lastKey, 10)] = this.parseValue(keyVal[fullKey])
            else curr[lastKey] = this.parseValue(keyVal[fullKey])
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