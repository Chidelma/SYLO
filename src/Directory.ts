import { rm, exists, mkdir, readdir, opendir, rmdir, watch, stat, symlink } from "node:fs/promises"
import Walker from "./Walker"
import ULID from "./ULID"
import { S3Client } from "bun"
import S3 from "./S3"

export default class {

    private static readonly KEY_LIMIT = 1024

    private static ALL_SCHEMAS = new Map<string, Map<string, string[]>>()

    private static readonly SLASH_ASCII = "%2F"

    private static retrieveSchema<T extends Record<string, any>>(doc: T, parentBranch?: string) {

        const schema = new Map<string, string>()
    
        function recursiveHelper(tree: Record<string, any>, parentBranch?: string) {

            for (const branch in tree) {

                const newKey = parentBranch ? `${parentBranch}/${branch}` : branch

                if(typeof tree[branch] === "object" && !Array.isArray(tree[branch])) {
                    recursiveHelper(tree[branch], newKey)
                } else schema.set(newKey, typeof tree[branch])
            }
        }
    
        recursiveHelper(doc, parentBranch)
        
        return schema
    }

    static async validateData<T extends Record<string, any>>(collection: string, data: T) {

        try {

            const savedSchema = this.ALL_SCHEMAS.get(collection)!

            const dataSchema = this.retrieveSchema(data)

            for(const [field, type] of dataSchema) {

                if(!savedSchema.has(field)) throw new Error(`Field '${field.split('/').pop()}' does not exist in '${collection}'`)

                if(!savedSchema.get(field)!.includes(type)) throw new Error(`Field '${field.split('/').pop()}' is not of type '${type}' in '${collection}'`)
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.validateData -> ${e.message}`)
        }
    }

    // static async aquireLock(collection: string, _id: _ulid) {

    //     try {

    //         if(await this.isLocked(collection, _id)) {

    //             await this.queueProcess(collection, _id)

    //             for await (const event of watch(`${Walker.DSK_DB}/${S3.getBucketFormat(collection)}/.${_id}/${process.pid}`)) {
    //                 if(event.eventType !== "change") break
    //             }
    //         }
    
    //         await this.queueProcess(collection, _id)

    //     } catch(e) {
    //         if(e instanceof Error) throw new Error(`Dir.aquireLock -> ${e.message}`)
    //     }

    // }

    // static async releaseLock(collection: string, _id: _ulid) {

    //     try {

    //         await rm(`${Walker.DSK_DB}/${S3.getBucketFormat(collection)}/.${_id}/${process.pid}`, { recursive: true })

    //         const results = await readdir(`${Walker.DSK_DB}/${S3.getBucketFormat(collection)}/.${_id}`, { withFileTypes: true })

    //         const timeSortedDir = results.sort((a, b) => {
    //             const aTime = Bun.file(`${Walker.DSK_DB}/${S3.getBucketFormat(collection)}/.${_id}/${a.name}`).lastModified
    //             const bTime = Bun.file(`${Walker.DSK_DB}/${S3.getBucketFormat(collection)}/.${_id}/${b.name}`).lastModified
    //             return aTime - bTime
    //         })

    //         if(timeSortedDir.length > 0) await rm(`${Walker.DSK_DB}/${S3.getBucketFormat(collection)}/.${_id}/${timeSortedDir[0].name}`, { recursive: true })
            
    //     } catch(e) {
    //         if(e instanceof Error) throw new Error(`Dir.releaseLock -> ${e.message}`)
    //     }
    // }

    // private static async isLocked(collection: string, _id: _ulid) {

    //     let locked = false

    //     try {

    //         if(!await exists(`${Walker.DSK_DB}/${S3.getBucketFormat(collection)}/.${_id}`)) return locked

    //         const files = await opendir(`${Walker.DSK_DB}/${S3.getBucketFormat(collection)}/.${_id}`)

    //         for await (const file of files) {

    //             if(!file.isSymbolicLink()) {
    //                 locked = true
    //                 break
    //             }

    //         }

    //     } catch(e) {
    //         if(e instanceof Error) throw new Error(`Dir.isLocked -> ${e.message}`)
    //     }

    //     return locked
    // }

    // private static async queueProcess(collection: string, _id: _ulid) {

    //     try {

    //         await mkdir(`${Walker.DSK_DB}/${S3.getBucketFormat(collection)}/.${_id}`, { recursive: true })

    //         await Bun.file(`${Walker.DSK_DB}/${S3.getBucketFormat(collection)}/.${_id}/${process.pid}`).writer().end()

    //     } catch(e) {
    //         if(e instanceof Error) throw new Error(`Dir.queueProcess -> ${e.message}`)
    //     }
    // }

    static async reconstructData<T extends Record<string, any>>(collection: string, items: string[]) {
        
        items = await this.readValues(collection, items)

        let fieldVal: Record<string, string> = {}

        items.forEach(data => {
            const segs = data.split('/')
            const val = segs.pop()!
            const field = segs.join('/')
            fieldVal = { ...fieldVal, [field]: val }
        })
        
        return this.constructData<T>(fieldVal)
    }

    private static async readValues(collection: string, items: string[]) {

        for(let i = 0; i < items.length; i++) {

            const segments = items[i].split('/')

            const filename = segments.pop()!

            if(ULID.isUUID(filename)) {

                const data = S3Client.file(items[i], { ...S3.CREDS, bucket: collection })
                
                const val = await data.text()

                items[i] = `${segments.join('/')}/${val}`
            }
        }

        return items
    }

    private static async filterByTimestamp(_id: _ulid, indexes: string[], { updated, created }: { updated?: _timestamp, created?: _timestamp }) {

        const { createdAt, updatedAt } = ULID.decodeTime(_id)
        
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

    static async *searchDocs<T extends Record<string, any>>(collection: string, pattern: string | string[], { updated, created }: { updated?: _timestamp, created?: _timestamp }, { listen = false, skip = false }: { listen: boolean, skip: boolean }, deleted: boolean = false): AsyncGenerator<Map<_ulid, T> | _ulid | void, void, { count: number, limit?: number  }> {
        
        const data = yield
        let count = data.count
        let limit = data.limit
        
        const constructData = async (collection: string, _id: _ulid, items: string[]) => {

            if(created || updated) {

                if(await this.filterByTimestamp(_id, items, { created, updated })) {

                    const data = await this.reconstructData<T>(collection, items)

                    return new Map([[_id, data]]) as Map<_ulid, T>

                } else return new Map<_ulid, T>()

            } else {

                const data = await this.reconstructData<T>(collection, items)

                return new Map([[_id, data]]) as Map<_ulid, T>
            }
        }

        const processQuery = async function*(p: string): AsyncGenerator<Map<_ulid, T> | _ulid | void, void, { count: number, limit?: number  }> {

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

    static async putKeys(collection: string, { dataKey, indexKey }: { dataKey: string, indexKey: string }) {
        
        let dataBody: string | undefined
        let indexBody: string | undefined

        if(dataKey.length > this.KEY_LIMIT) {

            const dataSegs = dataKey.split('/')

            dataBody = dataSegs.pop()!
            
            indexKey = `${dataSegs.join('/')}/${crypto.randomUUID()}`
        } 

        if(indexKey.length > this.KEY_LIMIT) {

            const indexSegs = indexKey.split('/')

            const _id = indexSegs.pop()! as _ulid

            indexBody = indexSegs.pop()!

            dataKey = `${indexSegs.join('/')}/${_id}`
        }

        await Promise.allSettled([
            S3.put(collection, dataKey, dataBody ?? ''),
            S3.put(collection, indexKey, indexBody ?? '')
        ])

        const _id = indexKey.split('/').pop()! as _ulid

        await symlink(``, `${Walker.DSK_DB}/${S3.getBucketFormat(collection)}/.${_id}/${indexKey.replaceAll('/', '\\')}`)
        await rm(`${Walker.DSK_DB}/${S3.getBucketFormat(collection)}/.${_id}/${indexKey.replaceAll('/', '\\')}`, { recursive: true })
    }

    static async deleteKeys(collection: string, dataKey: string) {

        const segements = dataKey.split('/')

        const val = segements.pop()!
        const _id = segements.shift()! as _ulid

        let index = `${segements.join('/')}/${val}/${_id}`

        if(ULID.isUUID(val)) index = `${segements.join('/')}/${_id}`

        await Promise.allSettled([
            S3.delete(collection, dataKey),
            S3.delete(collection, index)
        ])
    }

    static extractKeys<T>(_id: _ulid, data: T, parentField?: string) {

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

    static constructData<T>(fieldVal: Record<string, string>) {

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

        return data as T
    }

    static parseValue(value: string) {

        try {
            return JSON.parse(value)
        } catch(e) {
            return value
        }
    }
}