import { rm, exists, mkdir, readdir, opendir, rmdir, watch, stat, symlink } from "node:fs/promises"
import { PutObjectCommand, GetObjectCommand, DeleteObjectCommand, DeleteObjectsCommand, ListObjectsV2Command } from "@aws-sdk/client-s3"
import Walker from "./Walker"
import ULID from "./ULID"

export default class {

    private static readonly KEY_LIMIT = 1024

    private static SCHEMA_PATH = process.env.SCHEMA_PATH || `${process.cwd()}/schemas`

    private static ALL_SCHEMAS = new Map<string, Map<string, string[]>>()

    private static readonly SLASH_ASCII = "%2F"

    static async createSchema(collection: string) {

        try {

            const savedSchema = Bun.file(`${Walker.DSK_DB}/${collection}/.schema.json`)

            if(await savedSchema.exists()) throw new Error(`Cannot create schema for '${collection}' as it already exists`)

            const schemaFile = Bun.file(`${this.SCHEMA_PATH}/${collection}.d.ts`)

            if(!await schemaFile.exists()) throw new Error(`Cannot find declaration file for '${collection}'`)

            const newSchema = this.getSchema(collection, await schemaFile.text())
            
            await Bun.write(savedSchema, JSON.stringify(Object.fromEntries(newSchema)))

            this.ALL_SCHEMAS.set(collection, newSchema)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.createSchema -> ${e.message}`)
        }
    }

    static async modifySchema(collection: string) {

        try {

            const schemaFile = Bun.file(`${this.SCHEMA_PATH}/${collection}.d.ts`)

            if(!await schemaFile.exists()) throw new Error(`Cannot find declaration file for '${collection}'`)

            const savedSchemaFile = Bun.file(`${Walker.DSK_DB}/${collection}/.schema.json`)

            if(!await savedSchemaFile.exists()) throw new Error(`Cannot find saved schema for '${collection}'`)

            const savedSchemaData: Record<string, string[]> = await savedSchemaFile.json()

            if(schemaFile.lastModified > savedSchemaFile.lastModified) {

                const newSchemaData = this.getSchema(collection, await savedSchemaFile.text())

                const newSchemaFields = new Set(Object.keys(newSchemaData))

                const savedSchemaFields = new Set(Object.keys(savedSchemaData))

                const removedFields = savedSchemaFields.difference(newSchemaFields)

                for(const field of removedFields) {

                    let idxToken: string | undefined

                    do {

                        const idxResults = await Walker.s3Client.send(new ListObjectsV2Command({
                            Bucket: Walker.S3_IDX_DB,
                            Prefix: `${collection}/${field}`,
                            ContinuationToken: idxToken
                        }))

                        idxToken = idxResults.NextContinuationToken

                        if(!idxResults.Contents) break

                        await Walker.s3Client.send(new DeleteObjectsCommand({
                            Bucket: Walker.S3_IDX_DB,
                            Delete: {
                                Objects: idxResults.Contents!.map(content => ({ Key: content.Key! })),
                                Quiet: false
                            }
                        }))

                    } while(idxToken)
                }

                await Bun.write(savedSchemaFile, JSON.stringify(Object.fromEntries(newSchemaData)))
                
                this.ALL_SCHEMAS.set(collection, newSchemaData)
            }

            if(!this.ALL_SCHEMAS.has(collection)) this.ALL_SCHEMAS.set(collection, new Map(Object.entries(savedSchemaData)))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.modifySchema -> ${e.message}`)
        }
    }

    static async dropSchema(collection: string) {

        try {

            let idxToken: string | undefined

            do {

                const data = await Walker.s3Client.send(new ListObjectsV2Command({
                    Bucket: Walker.S3_IDX_DB,
                    Prefix: collection,
                    ContinuationToken: idxToken
                }))

                idxToken = data.NextContinuationToken

                if(!data.Contents) break

                await Walker.s3Client.send(new DeleteObjectsCommand({
                    Bucket: Walker.S3_IDX_DB,
                    Delete: {
                        Objects: data.Contents!.map(content => ({ Key: content.Key! })),
                        Quiet: false
                    }
                }))

            } while(idxToken)

            let dataToken: string | undefined

            do {

                const data = await Walker.s3Client.send(new ListObjectsV2Command({
                    Bucket: Walker.S3_DATA_DB,
                    Prefix: collection,
                    ContinuationToken: idxToken
                }))

                dataToken = data.NextContinuationToken

                if(!data.Contents) break

                await Walker.s3Client.send(new DeleteObjectsCommand({
                    Bucket: Walker.S3_DATA_DB,
                    Delete: {
                        Objects: data.Contents!.map(content => ({ Key: content.Key! })),
                        Quiet: false
                    }
                }))

            } while(dataToken)

            await rmdir(`${Walker.DSK_DB}/${collection}`, { recursive: true })

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.dropSchema -> ${e.message}`)
        }
    }

    private static parseLines(lines: string[], startIndex: number = 0): [Record<string, any>, number] {
    
        const obj: Record<string, any> = {}
        let i = startIndex
    
        while (i < lines.length) {
            
            const current = lines[i].trim()
            
            if (current === '}') return [obj, i]
            
            const colonIndex = current.indexOf(':')
            if (colonIndex === -1) {
                i++
                continue
            }
            
            const field = current.slice(0, colonIndex).trim();
            const type = current.slice(colonIndex + 1).trim();
            
            if (type === '{') {
                const [children, newIndex] = this.parseLines(lines, i + 1)
                obj[field] = children
                i = newIndex
            } else obj[field] = type
            
            i++
        }
        
        return [obj, i]
    }
    
    private static constructSchema(tree: Record<string, any>, parentBranch?: string) {
    
        const schema = new Map<string, string[]>()
    
        function recursiveHelper(tree: Record<string, any>, parentBranch?: string) {

            for (const branch in tree) {

                const newKey = parentBranch ? `${parentBranch}/${branch}` : branch
                
                if (typeof tree[branch] === 'string') {
                    const types = (tree[branch] as string).split('|').map(type => type.trim() === 'null' ? 'object' : type.trim())
                    schema.set(newKey, types)
                } else if (typeof tree[branch] === 'object') {
                    recursiveHelper(tree[branch], newKey)
                }
            }
        }
    
        recursiveHelper(tree, parentBranch)
        
        return schema
    }

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
    
    private static getSchema(collection: string, schemaData: string) {

        const match = schemaData.match(/{([\s\S]*)}/)

        if(!match) throw new Error(`declaration file for '${collection}' not formatted correctly`)

        const [_, yaml] = match

        const lines = yaml.replaceAll(',', '').split('\n').map(line => line.trim()).filter(line => line.length > 0)

        const [ tree ] = this.parseLines(lines)

        return this.constructSchema(tree)
    }

    static async validateData<T extends Record<string, any>>(collection: string, data: T) {

        try {

            await this.modifySchema(collection)

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

    static async aquireLock(collection: string, _id: _ulid) {

        try {

            if(await this.isLocked(collection, _id)) {

                await this.queueProcess(collection, _id)

                for await (const event of watch(`${Walker.DSK_DB}/${collection}/.${_id}/${process.pid}`)) {
                    if(event.eventType !== "change") break
                }
            }
    
            await this.queueProcess(collection, _id)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.aquireLock -> ${e.message}`)
        }

    }

    static async releaseLock(collection: string, _id: _ulid) {

        try {

            await rm(`${Walker.DSK_DB}/${collection}/.${_id}/${process.pid}`, { recursive: true })

            const results = await readdir(`${Walker.DSK_DB}/${collection}/.${_id}`, { withFileTypes: true })

            const timeSortedDir = results.sort((a, b) => {
                const aTime = Bun.file(`${Walker.DSK_DB}/${collection}/.${_id}/${a.name}`).lastModified
                const bTime = Bun.file(`${Walker.DSK_DB}/${collection}/.${_id}/${b.name}`).lastModified
                return aTime - bTime
            })

            if(timeSortedDir.length > 0) await rm(`${Walker.DSK_DB}/${collection}/.${_id}/${timeSortedDir[0].name}`, { recursive: true })
            
        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.releaseLock -> ${e.message}`)
        }
    }

    private static async isLocked(collection: string, _id: _ulid) {

        let locked = false

        try {

            if(!await exists(`${Walker.DSK_DB}/${collection}/.${_id}`)) return locked

            const files = await opendir(`${Walker.DSK_DB}/${collection}/.${_id}`)

            for await (const file of files) {

                if(!file.isSymbolicLink()) {
                    locked = true
                    break
                }

            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.isLocked -> ${e.message}`)
        }

        return locked
    }

    private static async queueProcess(collection: string, _id: _ulid) {

        try {

            await mkdir(`${Walker.DSK_DB}/${collection}/.${_id}`, { recursive: true })

            await Bun.file(`${Walker.DSK_DB}/${collection}/.${_id}/${process.pid}`).writer().end()

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.queueProcess -> ${e.message}`)
        }
    }

    static async reconstructData<T extends Record<string, any>>(items: string[]) {
        
        items = await this.readValues(items)

        let fieldVal: Record<string, string> = {}

        items.forEach(data => {
            const segs = data.split('/')
            const val = segs.pop()!
            const field = segs.join('/')
            fieldVal = { ...fieldVal, [field]: val }
        })
        
        return this.constructData<T>(fieldVal)
    }

    static async readValues(items: string[]) {

        for(let i = 0; i < items.length; i++) {

            const segments = items[i].split('/')

            const filename = segments.pop()!

            if(ULID.isUUID(filename)) {

                const data = await Walker.s3Client.send(new GetObjectCommand({
                    Bucket: Walker.S3_DATA_DB,
                    Key: items[i]
                }))

                const val = await data.Body?.transformToString('utf-8') || ''

                items[i] = `${segments.join('/')}/${val}`
            }
        }

        return items
    }

    private static async filterByTimestamp(collection: string, _id: _ulid, indexes: string[], { updated, created }: { updated?: _timestamp, created?: _timestamp }) {

        if(updated && await exists(`${Walker.DSK_DB}/${collection}/.${_id}`)) {

            const metadata = await stat(`${Walker.DSK_DB}/${collection}/.${_id}`)

            const lastModified = metadata.mtime.getMilliseconds()

            if((updated.$gt || updated.$gte) && (updated.$lt || updated.$lte)) {

                if(updated.$gt && updated.$lt) {

                    if(updated.$gt! > updated.$lt!) throw new Error("Invalid updated query")

                    indexes = lastModified > updated.$gt! && lastModified < updated.$lt! ? indexes : []
                
                } else if(updated.$gt && updated.$lte) {

                    if(updated.$gt! > updated.$lte!) throw new Error("Invalid updated query")

                    indexes = lastModified > updated.$gt! && lastModified <= updated.$lte! ? indexes : []
                
                } else if(updated.$gte && updated.$lt) {

                    if(updated.$gte! > updated.$lt!) throw new Error("Invalid updated query")

                    indexes = lastModified >= updated.$gte! && lastModified < updated.$lt! ? indexes : []
                
                } else if(updated.$gte && updated.$lte) {

                    if(updated.$gte! > updated.$lte!) throw new Error("Invalid updated query")

                    indexes = lastModified >= updated.$gte! && lastModified <= updated.$lte! ? indexes : []
                }

            } else if((updated.$gt || updated.$gte) && !updated.$lt && !updated.$lte) {

                indexes = updated.$gt ? lastModified > updated.$gt! ? indexes : [] : lastModified >= updated.$gte! ? indexes : []
            
            } else if(!updated.$gt && !updated.$gte && (updated.$lt || updated.$lte)) {

                indexes = updated.$lt ? lastModified < updated.$lt! ? indexes : [] : lastModified <= updated.$lte! ? indexes : []
            }
        }

        if(created) {

            if((created.$gt || created.$gte) && (created.$lt || created.$lte)) {

                if(created.$gt && created.$lt) {

                    if(created.$gt! > created.$lt!) throw new Error("Invalid created query")

                    const creation = ULID.decodeTime(_id)
                    indexes = creation > created.$gt! && creation < created.$lt! ? indexes : []
                
                } else if(created.$gt && created.$lte) {

                    if(created.$gt! > created.$lte!) throw new Error("Invalid updated query")

                    const creation = ULID.decodeTime(_id)
                    indexes = creation > created.$gt! && creation <= created.$lte! ? indexes : []
                
                } else if(created.$gte && created.$lt) {

                    if(created.$gte! > created.$lt!) throw new Error("Invalid updated query")

                    const creation = ULID.decodeTime(_id)
                    indexes = creation >= created.$gte! && creation < created.$lt! ? indexes : []
                
                } else if(created.$gte && created.$lte) {

                    if(created.$gte! > created.$lte!) throw new Error("Invalid updated query")

                    const creation = ULID.decodeTime(_id)
                    indexes = creation >= created.$gte! && creation <= created.$lte! ? indexes : []
                }

            } else if((created.$gt || created.$gte) && !created.$lt && !created.$lte) {

                const creation = ULID.decodeTime(_id)

                if(created.$gt) indexes = creation > created.$gt! ? indexes : []
                else if(created.$gte) indexes = creation >= created.$gte! ? indexes : []
            
            } else if(!created.$gt && !created.$gte && (created.$lt || created.$lte)) {

                const creation = ULID.decodeTime(_id)

                if(created.$lt) indexes = creation < created.$lt! ? indexes : []
                else if(created.$lte) indexes = creation <= created.$lte! ? indexes : []
            }
        }

        return indexes.length > 0
    }

    static async *searchDocs<T extends Record<string, any>>(pattern: string | string[], { updated, created }: { updated?: _timestamp, created?: _timestamp }, { listen = false, skip = false }: { listen: boolean, skip: boolean }, deleted: boolean = false): AsyncGenerator<Map<_ulid, T> | _ulid | void, void, { count: number, limit?: number  }> {
        
        const data = yield
        let count = data.count
        let limit = data.limit
        
        const constructData = async (collection: string, _id: _ulid, items: string[]) => {

            if(created || updated) {

                if(await this.filterByTimestamp(collection, _id, items, { created, updated })) {

                    const data = await this.reconstructData<T>(items)

                    return new Map([[_id, data]]) as Map<_ulid, T>

                } else return new Map<_ulid, T>()

            } else {

                const data = await this.reconstructData<T>(items)

                return new Map([[_id, data]]) as Map<_ulid, T>
            }
        }

        const processQuery = async function*(p: string): AsyncGenerator<Map<_ulid, T> | _ulid | void, void, { count: number, limit?: number  }> {

            let finished = false
            
            if(listen && !deleted) {

                const iter = Walker.search(p, { listen, skip })

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

                const iter = Walker.search(p, { listen, skip }, "delete")

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

                const iter = Walker.search(p, { listen, skip })

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

    static async putKeys({ data, index }: { data: string, index: string }) {

        let dataBody: string | undefined
        let indexBody: string | undefined

        if(data.length > this.KEY_LIMIT) {

            const dataSegs = data.split('/')

            dataBody = dataSegs.pop()!
            
            index = `${dataSegs.join('/')}/${crypto.randomUUID()}`
        } 

        if(index.length > this.KEY_LIMIT) {

            const indexSegs = index.split('/')

            const _id = indexSegs.pop()! as _ulid

            indexBody = indexSegs.pop()!

            data = `${indexSegs.join('/')}/${_id}`
        }

        await Promise.allSettled([Walker.s3Client.send(new PutObjectCommand({
            Bucket: Walker.S3_DATA_DB,
            Key: data,
            Body: dataBody,
            ContentLength: dataBody ? dataBody.length : 0
        })), Walker.s3Client.send(new PutObjectCommand({
            Bucket: Walker.S3_IDX_DB,
            Key: index,
            Body: indexBody,
            ContentLength: indexBody ? indexBody.length : 0
        }))])

        const collection = index.split('/').shift()!
        const _id = index.split('/').pop()! as _ulid

        await symlink(``, `${Walker.DSK_DB}/${collection}/.${_id}/${index.replaceAll('/', '\\')}`)
        await rm(`${Walker.DSK_DB}/${collection}/.${_id}/${index.replaceAll('/', '\\')}`, { recursive: true })
    }

    static async deleteKeys(dataKey: string) {

        const segements = dataKey.split('/')

        const val = segements.pop()!
        const collection = segements.shift()!
        const _id = segements.shift()! as _ulid

        let index = `${collection}/${segements.join('/')}/${val}/${_id}`

        if(ULID.isUUID(val)) index = `${collection}/${segements.join('/')}/${_id}`

        await Promise.allSettled([Walker.s3Client.send(new DeleteObjectCommand({
            Bucket: Walker.S3_DATA_DB,
            Key: dataKey
        })), Walker.s3Client.send(new DeleteObjectCommand({
            Bucket: Walker.S3_IDX_DB,
            Key: index
        }))])
    }

    static extractKeys<T>(collection: string, _id: _ulid, data: T, parentField?: string) {

        const keys: { data: string[], indexes: string[] } = { data: [], indexes: [] }

        const obj = {...data}

        for(const field in obj) {

            const newField = parentField ? `${parentField}/${field}` : field

            if(typeof obj[field] === 'object' && !Array.isArray(obj[field])) {
                const items = this.extractKeys(collection, _id, obj[field], newField)
                keys.data.push(...items.data)
                keys.indexes.push(...items.indexes)
            } else if(typeof obj[field] === 'object' && Array.isArray(obj[field])) {
                const items: (string | number | boolean)[] = obj[field]
                if(items.some((item) => typeof item === 'object')) throw new Error(`Cannot have an array of objects`)
                keys.data.push(`${collection}/${_id}/${newField}/${JSON.stringify(items).replaceAll('/', this.SLASH_ASCII)}`)
                keys.indexes.push(`${collection}/${newField}/${JSON.stringify(items).replaceAll('/', this.SLASH_ASCII)}/${_id}`)
            } else {
                keys.data.push(`${collection}/${_id}/${newField}/${String(obj[field]).replaceAll('/', this.SLASH_ASCII)}`)
                keys.indexes.push(`${collection}/${newField}/${String(obj[field]).replaceAll('/', this.SLASH_ASCII)}/${_id}`)
            }
        }

        return keys
    }

    static constructData<T>(fieldVal: Record<string, string>) {

        const data: Record<string, any> = {}

        for(let fullField in fieldVal) {

            const fields = fullField.split('/').slice(2)

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