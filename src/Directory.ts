import { rm, exists, mkdir, rmdir, symlink, readdir } from "node:fs/promises"
import Walker from "./Walker"
import ULID from "./ULID"

export default class {

    private static readonly CHAR_LIMIT = 255

    private static readonly SLASH_ASCII = "%2F"

    private static SCHEMA_PATH = process.env.SCHEMA_PATH || `${process.cwd()}/schemas`

    private static ALL_SCHEMAS = new Map<string, Map<string, string[]>>()

    static async createDirFile(dirname: string, filename: any, data?: string) {

        try {

            const collection = dirname.split('/').shift()!

            const index = `${Walker.DSK_DB}/${dirname}/${filename}`

            await mkdir(`${Walker.DSK_DB}/${dirname}`, { recursive: true })

            if(data) {
                const writer = Bun.file(index).writer()
                writer.write(data)
                await writer.end()
            } else await Bun.file(index).writer().end()

            await symlink(index, `${Walker.DSK_DB}/${collection}/${Walker.DIRECT_DIR}/${filename}/${crypto.randomUUID()}`, 'file')

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.createDirFile -> ${e.message}`)
        }
    }

    static async createSchema(collection: string) {

        try {

            const savedSchema = Bun.file(`${Walker.DSK_DB}/${collection}/.schema.json`)

            if(await savedSchema.exists()) throw new Error(`Cannot create schema for '${collection}' as it already exists`)

            const schemaFile = Bun.file(`${this.SCHEMA_PATH}/${collection}.d.ts`)

            if(!await schemaFile.exists()) throw new Error(`Cannot finddeclaration file for '${collection}'`)

            const newSchema = this.getSchema(collection, await schemaFile.text())
            
            await Bun.write(savedSchema, JSON.stringify(Object.fromEntries(newSchema)))

            this.ALL_SCHEMAS.set(collection, newSchema)

            for(const field of newSchema.keys()) {

                await mkdir(`${Walker.DSK_DB}/${collection}/${field}`, { recursive: true })
            }

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

                const addedFields = newSchemaFields.difference(savedSchemaFields)

                for(const field of removedFields) await rm(`${Walker.DSK_DB}/${collection}/${field}`, { recursive: true })

                for(const field of addedFields) await mkdir(`${Walker.DSK_DB}/${collection}/${field}`, { recursive: true })

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
    
                for await (const event of Walker.listen(`${collection}/${Walker.DIRECT_DIR}/${_id}/${process.pid}`)) {
                    if(event.action === "delete") break
                }
            }
    
            await this.queueProcess(collection, _id)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.aquireLock -> ${e.message}`)
        }
    }

    static async releaseLock(collection: string, _id: _ulid) {

        try {

            await rm(`${Walker.DSK_DB}/${collection}/${Walker.DIRECT_DIR}/${_id}/${process.pid}`, { recursive: true })

            const results = await readdir(`${Walker.DSK_DB}/${collection}/${Walker.DIRECT_DIR}/${_id}`, { withFileTypes: true })

            const timeSortedDir = results.filter(p => !p.isSymbolicLink()).sort((a, b) => {
                const aTime = Bun.file(`${Walker.DSK_DB}/${collection}/${Walker.DIRECT_DIR}/${_id}/${a.name}`).lastModified
                const bTime = Bun.file(`${Walker.DSK_DB}/${collection}/${Walker.DIRECT_DIR}/${_id}/${b.name}`).lastModified
                return aTime - bTime
            })

            if(timeSortedDir.length > 0) await rm(`${Walker.DSK_DB}/${collection}/${Walker.DIRECT_DIR}/${_id}/${timeSortedDir[0].name}`, { recursive: true })

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.releaseLock -> ${e.message}`)
        }
    }

    private static async isLocked(collection: string, _id: _ulid) {

        const results = await Array.fromAsync(new Bun.Glob(`${collection}/${Walker.DIRECT_DIR}/${_id}/**/*`).scan({ cwd: Walker.DSK_DB }))
        
        return results.filter(p => !ULID.isUUID(p.split('/').pop()!)).length > 0
    }

    private static async queueProcess(collection: string, _id: _ulid) {

        try {

            await mkdir(`${Walker.DSK_DB}/${collection}/${Walker.DIRECT_DIR}/${_id}`, { recursive: true })

            await Bun.file(`${Walker.DSK_DB}/${collection}/${Walker.DIRECT_DIR}/${_id}/${process.pid}`).writer().end()

        } catch(e) {
            if(e instanceof Error) throw new Error(`Dir.queueProcess -> ${e.message}`)
        }
    }

    static async reconstructData<T extends Record<string, any>>(indexes: string[]) {
        
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

            const file = Bun.file(`${Walker.DSK_DB}/${index}`)

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

    private static filterByTimestamp(_id: _ulid, indexes: string[], { updated, created }: { updated?: _timestamp, created?: _timestamp }) {

        if(updated) {

            if((updated.$gt || updated.$gte) && (updated.$lt || updated.$lte)) {

                if(updated.$gt && updated.$lt) {

                    if(updated.$gt! > updated.$lt!) throw new Error("Invalid updated query")

                    indexes = indexes.filter(idx => {
                        return Bun.file(`${Walker.DSK_DB}/${idx}`).lastModified > updated.$gt! && Bun.file(`${Walker.DSK_DB}/${idx}`).lastModified < updated.$lt!
                    })
                
                } else if(updated.$gt && updated.$lte) {

                    if(updated.$gt! > updated.$lte!) throw new Error("Invalid updated query")

                    indexes = indexes.filter(idx => {
                        return Bun.file(`${Walker.DSK_DB}/${idx}`).lastModified > updated.$gt! && Bun.file(`${Walker.DSK_DB}/${idx}`).lastModified <= updated.$lte!
                    })
                
                } else if(updated.$gte && updated.$lt) {

                    if(updated.$gte! > updated.$lt!) throw new Error("Invalid updated query")

                    indexes = indexes.filter(idx => {
                        return Bun.file(`${Walker.DSK_DB}/${idx}`).lastModified >= updated.$gte! && Bun.file(`${Walker.DSK_DB}/${idx}`).lastModified < updated.$lt!
                    })
                
                } else if(updated.$gte && updated.$lte) {

                    if(updated.$gte! > updated.$lte!) throw new Error("Invalid updated query")

                    indexes = indexes.filter(idx => {
                        return Bun.file(`${Walker.DSK_DB}/${idx}`).lastModified >= updated.$gte! && Bun.file(`${Walker.DSK_DB}/${idx}`).lastModified <= updated.$lte!
                    })
                }

            } else if((updated.$gt || updated.$gte) && !updated.$lt && !updated.$lte) {

                indexes = indexes.filter(idx => {
                    return updated.$gt ? Bun.file(`${Walker.DSK_DB}/${idx}`).lastModified > updated.$gt! : Bun.file(`${Walker.DSK_DB}/${idx}`).lastModified >= updated.$gte!
                })
            
            } else if(!updated.$gt && !updated.$gte && (updated.$lt || updated.$lte)) {

                indexes = indexes.filter(idx => {
                    return updated.$lt ? Bun.file(`${Walker.DSK_DB}/${idx}`).lastModified < updated.$lt! : Bun.file(`${Walker.DSK_DB}/${idx}`).lastModified <= updated.$lte!
                })
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

    static async *searchDocs<T extends Record<string, any>>(pattern: string | string[], { updated, created }: { updated?: _timestamp, created?: _timestamp }, listen: boolean = false, deleted: boolean = false) {
        
        const constructData = async (_id: _ulid, indexes: string[]) => {

            if(created || updated) {

                if(this.filterByTimestamp(_id, indexes, { created, updated })) {

                    const data = await this.reconstructData<T>(indexes)

                    return new Map([[_id, data]]) as Map<_ulid, T>

                } else return new Map<_ulid, T>()

            } else {

                const data = await this.reconstructData<T>(indexes)

                return new Map([[_id, data]]) as Map<_ulid, T>
            }
        }

        if(Array.isArray(pattern)) {

            for(const p of pattern) {

                if(listen && !deleted) {

                    for await(const { _id, docIndexes } of Walker.search(p, listen)) {

                        yield await constructData(_id, docIndexes)
                    }

                } else if(listen && deleted) {

                    for await(const { _id } of Walker.search(p, listen, "delete")) {

                        yield _id
                    }

                } else {

                    for await(const { _id, docIndexes } of Walker.search(p, listen)) {

                        yield await constructData(_id, docIndexes)
                    }
                }
            }

        } else {

            if(listen && !deleted) {

                for await(const { _id, docIndexes } of Walker.search(pattern, listen)) {

                    yield await constructData(_id, docIndexes)
                }

            } else if(listen && deleted) {

                for await(const { _id } of Walker.search(pattern, listen, "delete")) {

                    yield _id
                }

            } else {

                for await(const { _id, docIndexes } of Walker.search(pattern, listen)) {

                    yield await constructData(_id, docIndexes)
                }
            }
        }
    }

    static async putIndex(index: string) {

        const segements = index.split('/')

        const collection = segements.shift()!
        const _id = segements.pop()! as _ulid
        const val = segements.pop()!
        
        if(val.length > this.CHAR_LIMIT) {
            index = `${collection}/${segements.join('/')}/${_id}`
            await this.createDirFile(`${collection}/${segements.join('/')}`, _id, val)
        }
        else {
            index = `${collection}/${segements.join('/')}/${val}/${_id}`
            await this.createDirFile(`${collection}/${segements.join('/')}/${val}`, _id)
        }

        return index
    }

    static async deleteIndex(index: string) { 

        const fullIndex = `${Walker.DSK_DB}/${index}`

        if(await exists(fullIndex)) await rm(fullIndex, { recursive: true })
    }

    static deconstructData<T>(collection: string, _id: _ulid, data: T, parentField?: string) {

        const indexes: string[] = []

        const obj = {...data}

        for(const field in obj) {

            const newField = parentField ? `${parentField}/${field}` : field

            if(typeof obj[field] === 'object' && !Array.isArray(obj[field])) {
                indexes.push(...this.deconstructData(collection, _id, obj[field], newField))
            } else if(typeof obj[field] === 'object' && Array.isArray(obj[field])) {
                const items: (string | number | boolean)[] = obj[field]
                if(items.some((item) => typeof item === 'object')) throw new Error(`Cannot have an array of objects`)
                if(JSON.stringify(items).length > this.CHAR_LIMIT) throw new Error(`Field '${items.join('')}' is too long`)
                indexes.push(`${collection}/${newField}/${JSON.stringify(items).replaceAll('/', this.SLASH_ASCII)}/${_id}`)
            } else {
                if(String(obj[field]).length > this.CHAR_LIMIT) throw new Error(`Field '${obj[field]}' is too long`)
                indexes.push(`${collection}/${newField}/${String(obj[field]).replaceAll('/', this.SLASH_ASCII)}/${_id}`)
            }
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