import Walker from "./walker"
import TTID from "@vyckr/ttid"
import S3 from "./S3"
import Redis from "./redis"

export default class Dir {

    private static readonly KEY_LIMIT = 1024

    private static readonly SLASH_ASCII = "%2F"

    private readonly transactions: Array<{ action: Function, args: string[] }>;

    private static readonly redis = new Redis()
    
    constructor() {
        this.transactions = []
    }

    static async reconstructData(collection: string, items: string[]) {
        
        items = await this.readValues(collection, items)

        let fieldVal: Record<string, string> = {}

        items.forEach(data => {
            const segs = data.split('/')
            const val = segs.pop()!
            const field = segs.join('/')
            fieldVal = { ...fieldVal, [field]: val }
        })
        
        return this.constructData(fieldVal)
    }

    private static async readValues(collection: string, items: string[]) {

        for(let i = 0; i < items.length; i++) {

            const segments = items[i].split('/')

            const filename = segments.pop()!

            if(TTID.isUUID(filename)) {

                const file = S3.file(collection, items[i])
                const val = await file.text()

                items[i] = `${segments.join('/')}/${val}`
            }
        }

        return items
    }

    private static async filterByTimestamp(_id: _ttid, indexes: string[], { updated, created }: { updated?: _timestamp, created?: _timestamp }) {

        const { createdAt, updatedAt } = TTID.decodeTime(_id)
        
        if(updated) {

            if((updated.$gt || updated.$gte) && (updated.$lt || updated.$lte)) {

                if(updated.$gt && updated.$lt) {

                    if(updated.$gt! > updated.$lt!) throw new Error("Invalid updated query")

                    indexes = updatedAt > updated.$gt! && updatedAt < updated.$lt! ? indexes : []
                
                } else if(updated.$gt && updated.$lte) {

                    if(updated.$gt! > updated.$lte!) throw new Error("Invalid updated query")

                    indexes = updatedAt > updated.$gt! && updatedAt <= updated.$lte! ? indexes : []
                
                } else if(updated.$gte && updated.$lt) {

                    if(updated.$gte! > updated.$lt!) throw new Error("Invalid updated query")

                    indexes = updatedAt >= updated.$gte! && updatedAt < updated.$lt! ? indexes : []
                
                } else if(updated.$gte && updated.$lte) {

                    if(updated.$gte! > updated.$lte!) throw new Error("Invalid updated query")

                    indexes = updatedAt >= updated.$gte! && updatedAt <= updated.$lte! ? indexes : []
                }

            } else if((updated.$gt || updated.$gte) && !updated.$lt && !updated.$lte) {

                indexes = updated.$gt ? updatedAt > updated.$gt! ? indexes : [] : updatedAt >= updated.$gte! ? indexes : []
            
            } else if(!updated.$gt && !updated.$gte && (updated.$lt || updated.$lte)) {

                indexes = updated.$lt ? updatedAt < updated.$lt! ? indexes : [] : updatedAt <= updated.$lte! ? indexes : []
            }
        }

        if(created) {

            if((created.$gt || created.$gte) && (created.$lt || created.$lte)) {

                if(created.$gt && created.$lt) {

                    if(created.$gt! > created.$lt!) throw new Error("Invalid created query")

                    indexes = createdAt > created.$gt! && createdAt < created.$lt! ? indexes : []
                
                } else if(created.$gt && created.$lte) {

                    if(created.$gt! > created.$lte!) throw new Error("Invalid updated query")

                    indexes = createdAt > created.$gt! && createdAt <= created.$lte! ? indexes : []
                
                } else if(created.$gte && created.$lt) {

                    if(created.$gte! > created.$lt!) throw new Error("Invalid updated query")

                    indexes = createdAt >= created.$gte! && createdAt < created.$lt! ? indexes : []
                
                } else if(created.$gte && created.$lte) {

                    if(created.$gte! > created.$lte!) throw new Error("Invalid updated query")

                    indexes = createdAt >= created.$gte! && createdAt <= created.$lte! ? indexes : []
                }

            } else if((created.$gt || created.$gte) && !created.$lt && !created.$lte) {

                if(created.$gt) indexes = createdAt > created.$gt! ? indexes : []
                else if(created.$gte) indexes = createdAt >= created.$gte! ? indexes : []
            
            } else if(!created.$gt && !created.$gte && (created.$lt || created.$lte)) {

                if(created.$lt) indexes = createdAt < created.$lt! ? indexes : []
                else if(created.$lte) indexes = createdAt <= created.$lte! ? indexes : []
            }
        }

        return indexes.length > 0
    }

    static async *searchDocs<T extends Record<string, any>>(collection: string, pattern: string | string[], { updated, created }: { updated?: _timestamp, created?: _timestamp }, { listen = false, skip = false }: { listen: boolean, skip: boolean }, deleted: boolean = false): AsyncGenerator<Record<_ttid, T> | _ttid | void, void, { count: number, limit?: number  }> {
        
        const data = yield
        let count = data.count
        let limit = data.limit
        
        const constructData = async (collection: string, _id: _ttid, items: string[]) => {

            if(created || updated) {

                if(await this.filterByTimestamp(_id, items, { created, updated })) {

                    const data = await this.reconstructData(collection, items)

                    return { [_id]: data } as Record<_ttid, T>

                } else return {}

            } else {

                const data = await this.reconstructData(collection, items)

                return { [_id]: data } as Record<_ttid, T>
            }
        }

        const processQuery = async function*(p: string): AsyncGenerator<Record<_ttid, T> | _ttid | void, void, { count: number, limit?: number  }> {

            let finished = false
            
            if(listen && !deleted) {

                const iter = Walker.search(collection, p, { listen, skip })

                do {

                    const { value, done } = await iter.next({ count, limit })

                    if(done) finished = true

                    if(value) {
                        const data = yield await constructData(p.split('/').shift()!, value._id, value.data)
                        count = data.count
                        limit = data.limit
                    }

                } while(!finished)

            } else if(listen && deleted) {

                const iter = Walker.search(collection, p, { listen, skip }, "delete")

                do {

                    const { value, done } = await iter.next({ count, limit })

                    if(done) finished = true

                    if(value) {
                        const data = yield value._id
                        count = data.count
                        limit = data.limit
                    }

                } while(!finished)

            } else {

                const iter = Walker.search(collection, p, { listen, skip })

                do {

                    const { value, done } = await iter.next({ count, limit })

                    if(done) finished = true

                    if(value) {
                        const data = yield await constructData(p.split('/').shift()!, value._id, value.data)
                        count = data.count
                        limit = data.limit
                    }

                } while(!finished)
            }
        }

        if(Array.isArray(pattern)) {

            for(const p of pattern) yield* processQuery(p)

        } else yield* processQuery(pattern)
    }

    async putKeys(collection: string, { dataKey, indexKey }: { dataKey: string, indexKey: string }) {
        
        let dataBody: string | undefined
        let indexBody: string | undefined

        if(dataKey.length > Dir.KEY_LIMIT) {

            const dataSegs = dataKey.split('/')

            dataBody = dataSegs.pop()!
            
            indexKey = `${dataSegs.join('/')}/${Bun.randomUUIDv7()}`
        } 

        if(indexKey.length > Dir.KEY_LIMIT) {

            const indexSegs = indexKey.split('/')

            const _id = indexSegs.pop()! as _ttid

            indexBody = indexSegs.pop()!

            dataKey = `${indexSegs.join('/')}/${_id}`
        }

        await Promise.all([
            S3.put(collection, dataKey, dataBody ?? ''),
            S3.put(collection, indexKey, indexBody ?? '')
        ])

        this.transactions.push({
            action: S3.delete,
            args: [collection, dataKey]
        })

        this.transactions.push({
            action: S3.delete,
            args: [collection, indexKey]
        })

        await Dir.redis.publish(collection, 'insert', indexKey)
    }

    async executeRollback() {

        do {

            const transaction = this.transactions.pop()

            if(transaction) {

                const { action, args } = transaction

                await action(...args)
            }

        } while(this.transactions.length > 0)
    }

    async deleteKeys(collection: string, dataKey: string) {

        const segements = dataKey.split('/')

        const _id = segements.shift()!

        const indexKey = `${segements.join('/')}/${_id}`

        const dataFile = S3.file(collection, dataKey)
        const indexFile = S3.file(collection, indexKey)

        let dataBody: string | undefined
        let indexBody: string | undefined

        if(dataFile.size > 0) dataBody = await dataFile.text()
        if(indexFile.size > 0) indexBody = await indexFile.text()

        await Promise.all([
            S3.delete(collection, indexKey),
            S3.delete(collection, dataKey)
        ])

        this.transactions.push({
            action: S3.put,
            args: [collection, indexKey, dataBody ?? '']
        })

        this.transactions.push({
            action: S3.put,
            args: [collection, indexKey,  indexBody ?? '']
        })

        await Dir.redis.publish(collection, 'delete', _id)
    }

    static extractKeys<T>(_id: _ttid, data: T, parentField?: string) {

        const keys: { data: string[], indexes: string[] } = { data: [], indexes: [] }

        const obj = {...data}

        for(const field in obj) {

            const newField = parentField ? `${parentField}/${field}` : field

            if(typeof obj[field] === 'object' && !Array.isArray(obj[field])) {
                const items = this.extractKeys(_id, obj[field], newField)
                keys.data.push(...items.data)
                keys.indexes.push(...items.indexes)
            } else if(typeof obj[field] === 'object' && Array.isArray(obj[field])) {
                const items: (string | number | boolean)[] = obj[field]
                if(items.some((item) => typeof item === 'object')) throw new Error(`Cannot have an array of objects`)
                keys.data.push(`${_id}/${newField}/${JSON.stringify(items).replaceAll('/', this.SLASH_ASCII)}`)
                keys.indexes.push(`${newField}/${JSON.stringify(items).replaceAll('/', this.SLASH_ASCII)}/${_id}`)
            } else {
                keys.data.push(`${_id}/${newField}/${String(obj[field]).replaceAll('/', this.SLASH_ASCII)}`)
                keys.indexes.push(`${newField}/${String(obj[field]).replaceAll('/', this.SLASH_ASCII)}/${_id}`)
            }
        }

        return keys
    }

    static constructData(fieldVal: Record<string, string>) {

        const data: Record<string, any> = {}

        for(let fullField in fieldVal) {

            const fields = fullField.split('/').slice(1)

            let curr = data

            while(fields.length > 1) {

                const field = fields.shift()!

                if(typeof curr[field] !== 'object' || curr[field] === null) curr[field] = {}

                curr = curr[field]
            }

            const lastKey = fields.shift()!

            curr[lastKey] = this.parseValue(fieldVal[fullField].replaceAll(this.SLASH_ASCII, '/'))
        }

        return data
    }

    static parseValue(value: string) {

        try {
            return JSON.parse(value)
        } catch(e) {
            return value
        }
    }
}