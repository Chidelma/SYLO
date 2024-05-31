import { mkdirSync, rmSync, existsSync } from "fs"
import { glob } from "glob"
import { _keyval, _schema } from "../types/schema"
import { watch } from "chokidar"

class Queue {

    private UUID: string
    private lastId: string
    private currId: string
    private lastTime: number

    constructor() {
        this.UUID = crypto.randomUUID()
        this.lastId = ''
        this.currId = ''
        this.lastTime = Date.now()
    }

    private isNewChange() {
        return this.currId !== this.lastId || (this.currId === this.lastId && (Date.now() - this.lastTime) >= 1000)
    }

    dequeue(listener: (id: string) => void) {
        addEventListener(this.UUID, () => {
            if(this.isNewChange()) {
                listener(this.currId)
                this.lastId = this.currId
                this.lastTime = Date.now()
            }
        })
    }

    enqueue(id: string) {
        this.currId = id
        dispatchEvent(new Event(this.UUID))
    }
}

export default class {

    static readonly DATA_PATH = process.env.DATA_PREFIX ?? `${process.cwd()}/db`

    private static readonly uuidPattern = /[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/

    private static readonly ID_KEY = "_id"

    private static readonly CHAR_LIMIT = 255

    private static isIndex(path: string) {
        return path.split('/').length >= 4 && this.uuidPattern.test(path)
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

    static onAdd(pattern: string | string[], add: (id: string) => void) {

        const queue = new Queue()

        queue.dequeue(add)

        watch(pattern, { cwd: this.DATA_PATH }).on("addDir", path => {
            if(this.isIndex(path)) queue.enqueue(path.split('/').pop()!)
        })
    }

    static onDelete(pattern: string | string[], del: (id: string) => void) {

        const queue = new Queue()

        queue.dequeue(del)

        watch(pattern, { cwd: this.DATA_PATH }).on("unlinkDir", path => {
            if(this.isIndex(path)) queue.enqueue(path.split('/').pop()!)
        })
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