import { rmSync, existsSync, mkdirSync, rmdirSync } from "node:fs"
import Walker from "./Walker"

export default class {

    static readonly DB_PATH = process.env.DATA_PREFIX || `${process.cwd()}/db`

    private static readonly CHAR_LIMIT = 255

    private static readonly SLASH_ASCII = "%2F"

    private static SCHEMA_PATH = process.env.SCHEMA_PATH || `${process.cwd()}/schemas`

    private static ALL_SCHEMAS = new Map<string, Map<string, string[]>>()
 
    private static isUUID(id: string) {
        return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(id)
    }

    static async createSchema(collection: string) {

        try {

            const savedSchema = Bun.file(`${this.DB_PATH}/${collection}/.schema.json`)

            if(await savedSchema.exists()) throw new Error(`Cannot create schema for '${collection}' as it already exists`)

            const schemaFile = Bun.file(`${this.SCHEMA_PATH}/${collection}.d.ts`)

            if(!await schemaFile.exists()) throw new Error(`Cannot finddeclaration file for '${collection}'`)

            const newSchema = this.getSchema(collection, await schemaFile.text())
            
            await Bun.write(savedSchema, JSON.stringify(Object.fromEntries(newSchema)))

            this.ALL_SCHEMAS.set(collection, newSchema)

            for(const field of newSchema.keys()) mkdirSync(`${this.DB_PATH}/${collection}/${field}`, { recursive: true })

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.createSchema -> ${e.message}`)
        }
    }

    static async modifySchema(collection: string) {

        try {

            const schemaFile = Bun.file(`${this.SCHEMA_PATH}/${collection}.d.ts`)

            if(!await schemaFile.exists()) throw new Error(`Cannot find declaration file for '${collection}'`)

            const savedSchemaFile = Bun.file(`${this.DB_PATH}/${collection}/.schema.json`)

            if(!await savedSchemaFile.exists()) throw new Error(`Cannot find saved schema for '${collection}'`)

            const savedSchemaData: Record<string, string[]> = await savedSchemaFile.json()

            if(schemaFile.lastModified > savedSchemaFile.lastModified) {

                const newSchemaData = this.getSchema(collection, await savedSchemaFile.text())

                const newSchemaFields = new Set(Object.keys(newSchemaData))

                const savedSchemaFields = new Set(Object.keys(savedSchemaData))

                const removedFields = savedSchemaFields.difference(newSchemaFields)

                const addedFields = newSchemaFields.difference(savedSchemaFields)

                for(const field of removedFields) rmSync(`${this.DB_PATH}/${collection}/${field}`, { recursive: true })

                for(const field of addedFields) mkdirSync(`${this.DB_PATH}/${collection}/${field}`, { recursive: true })

                await Bun.write(savedSchemaFile, JSON.stringify(Object.fromEntries(newSchemaData)))
                
                this.ALL_SCHEMAS.set(collection, newSchemaData)
            }

            if(!this.ALL_SCHEMAS.has(collection)) this.ALL_SCHEMAS.set(collection, new Map(Object.entries(savedSchemaData)))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.modifySchema -> ${e.message}`)
        }
    }

    static dropSchema(collection: string) {

        try {

            rmdirSync(`${this.DB_PATH}/${collection}`, { recursive: true })

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

        const [__, yaml] = match

        const lines = yaml.replaceAll(',', '').split('\n').map(line => line.trim()).filter(line => line.length > 0)

        const [tree, _] = this.parseLines(lines)

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

    static async aquireLock(collection: string, _id: _uuid) {

        try {

            if(this.isLocked(collection, _id)) {

                await this.queueProcess(collection, _id)
    
                for await (const event of Walker.listen(`${collection}/.${_id}/${process.pid}`)) {
                    if(event.action === "delete") break
                }
            }
    
            await this.queueProcess(collection, _id)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.aquireLock -> ${e.message}`)
        }
    }

    static releaseLock(collection: string, _id: _uuid) {

        try {

            rmSync(`${this.DB_PATH}/${collection}/.${_id}/${process.pid}`, { recursive: true })

            const results = this.searchIndexes(`${collection}/.${_id}/**`)

            const timeSortedDir = results.sort((a, b) => {
                const aTime = Bun.file(`${this.DB_PATH}/${a}`).lastModified
                const bTime = Bun.file(`${this.DB_PATH}/${b}`).lastModified
                return aTime - bTime
            })

            if(timeSortedDir.length > 0) rmSync(`${this.DB_PATH}/${timeSortedDir[0]}`, { recursive: true })

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.releaseLock -> ${e.message}`)
        }
    }

    private static isLocked(collection: string, _id: _uuid) {

        const results = this.searchIndexes(`${collection}/.${_id}/**`)

        return results.filter(p => p.split('/').length === 3).length > 0
    }

    private static async queueProcess(collection: string, _id: _uuid) {

        await Bun.write(Bun.file(`${this.DB_PATH}/${collection}/.${_id}/${process.pid}`), '.')
    }

    static async reconstructData<T>(collection: string, _id: _uuid) {

        const indexes = this.searchIndexes(`${collection}/**/${_id}`)
        
        const fieldVals = await this.reArrangeIndexes(indexes)

        let fieldVal: Record<string, string> = {}

        fieldVals.forEach(data => {
            const segs = data.split('/')
            const val = segs.pop()!
            const field = segs.join('/')
            fieldVal = { ...fieldVal, [field]: val }
        })
        
        return this.constructData<T>(fieldVal)
    }

    static async reArrangeIndexes(indexes: string[]) {

        const keyVals: string[] = []

        for(const index of indexes) {

            let val: string

            const file = Bun.file(`${this.DB_PATH}/${index}`)

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

    static searchIndexes(pattern: string | string[]) {

        const indexes = new Set<string>()

        if(Array.isArray(pattern)) {
            for(const p of pattern) Walker.search(p).forEach(idx => indexes.add(idx))
        } else return Walker.search(pattern)

        return Array.from(indexes)
    }

    static async updateIndex(index: string) {

        const segements = index.split('/')

        const collection = segements.shift()!
        const field = segements.shift()!

        const _id = segements.pop()!
        const val = segements.pop()!

        const currIndexes = this.searchIndexes(`${collection}/${field}/**/${_id}`)

        currIndexes.forEach(idx => rmSync(`${this.DB_PATH}/${idx}`, { recursive: true }))

        if(val.length > this.CHAR_LIMIT) {
            await Bun.write(Bun.file(`${this.DB_PATH}/${collection}/${field}/${segements.join('/')}/${_id}`), val)
        } else {
            await Bun.write(Bun.file(`${this.DB_PATH}/${index}`), '.')
        }
    }

    static deleteIndex(index: string) { 
        if(existsSync(`${this.DB_PATH}/${index}`)) rmSync(`${this.DB_PATH}/${index}`, { recursive: true })
    }

    static deconstructData<T>(collection: string, _id: _uuid, data: T, parentField?: string) {

        const indexes: string[] = []

        const obj = {...data}

        for(const field in obj) {

            const newField = parentField ? `${parentField}/${field}` : field

            if(typeof obj[field] === 'object' && !Array.isArray(obj[field])) {
                indexes.push(...this.deconstructData(collection, _id, obj[field], newField))
            } else if(typeof obj[field] === 'object' && Array.isArray(obj[field])) {
                const items: (string | number | boolean)[] = obj[field]
                if(items.some((item) => typeof item === 'object')) throw new Error(`Cannot have an array of objects`)
                items.forEach((item, idx) => indexes.push(`${collection}/${newField}/${idx}/${String(item).replaceAll('/', this.SLASH_ASCII)}/${_id}`))
            } else indexes.push(`${collection}/${newField}/${String(obj[field]).replaceAll('/', this.SLASH_ASCII)}/${_id}`)
        }

        return indexes
    }

    static constructData<T>(fieldVal: Record<string, string>) {

        const data: Record<string, any> = {}

        for(let fullField in fieldVal) {

            const fields = fullField.split('/').slice(2)

            let curr = data

            while(fields.length > 1) {

                const field = fields.shift()!

                if(fields[0].match(/^\d+$/)) {
                    if(!Array.isArray(curr[field])) curr[field] = []
                } else {
                    if(typeof curr[field] !== 'object' || curr[field] === null) curr[field] = {}
                }

                curr = curr[field]
            }

            const lastKey = fields.shift()!

            if(lastKey.match(/^\d+$/)) curr[parseInt(lastKey, 10)] = this.parseValue(fieldVal[fullField].replaceAll(this.SLASH_ASCII, '/'))
            else curr[lastKey] = this.parseValue(fieldVal[fullField].replaceAll(this.SLASH_ASCII, '/'))
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