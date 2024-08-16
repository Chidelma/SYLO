import { FileChangeInfo, watch, mkdir, exists, opendir, readlink } from "node:fs/promises"
import { spawn } from "bun"
import ULID from "./ULID"

export default class Walker {

    private static readonly listeners: Map<string, AsyncIterable<FileChangeInfo<string>>> = new Map()

    static readonly DSK_DB = process.env.DB_DIR || `${process.cwd()}/db`

    static readonly TMP_DB = process.env.TMPFS

    static readonly DIRECT_DIR = '.direct'

    private static async *searchOnlyCollection(collection: string) {

        const uniqueIds = new Set<string>()

        const stream = spawn(['find', `${this.DSK_DB}/${collection}/${this.DIRECT_DIR}`, '-name', '*', '-type', 'd'], {
            stdin: 'pipe',
            stderr: 'pipe'
        })

        let isIncomplete = false    
        let incompletePath = ''

        for await (const chunk of stream.stdout) {

            const folders = new TextDecoder().decode(chunk).split('\n')

            for (let folder of folders) {

                if(isIncomplete) {
                    folder = incompletePath + folder
                    isIncomplete = false
                    incompletePath = ''
                }

                try {

                    const files = await opendir(folder)

                    const indexes: string[] = []
                    let _id: _ulid = '' as _ulid

                    for await (const file of files) {
                        if(!file.isSymbolicLink()) continue
                        const data = await readlink(`${folder}/${file.name}`)
                        _id = data.split('/').pop()! as _ulid
                        indexes.push(data.replace(`${this.DSK_DB}/`, ''))
                    }

                    if(!uniqueIds.has(_id)) yield { _id, docIndexes: indexes }

                    uniqueIds.add(_id)

                } catch(e) {
                    isIncomplete = true
                    incompletePath = folder
                }
            }
        } 
    }

    private static async *searchField(collection: string, pattern: string) {

        const uniqueIds = new Set<string>()

        const stream = spawn(['find', `${Walker.DSK_DB}/${collection}`, '-name', '*', '-type', 'f'], {
            stdin: 'pipe',
            stderr: 'pipe'
        })

        let isIncomplete = false    
        let incompletePath = ''

        for await (const chunk of stream.stdout) {

            const paths = new TextDecoder().decode(chunk).split('\n')

            for (const path of paths) {

                let subPath = path.replace(`${Walker.DSK_DB}/`, '')

                if(isIncomplete) {
                    subPath = incompletePath + subPath
                    isIncomplete = false
                    incompletePath = ''
                }

                if(!new Bun.Glob(pattern).match(subPath)) continue

                const segements = subPath.split('/')

                if(segements.length < 3) continue

                const _id = segements.pop()! as _ulid

                if(!ULID.isULID(_id)) {

                    isIncomplete = true
                    incompletePath = subPath

                    continue
                }

                if(!uniqueIds.has(_id)) yield { _id, docIndexes: await this.getDocIndexes(segements.shift()!, _id) }

                uniqueIds.add(_id)
            }
        } 
    }

    static async *search(pattern: string, listen: boolean = false, action: "upsert" | "delete" = "upsert") {

        const segments = pattern.split('/');
        const idx = segments.findIndex(seg => seg.includes('*'));
        const starter = segments.slice(0, idx).join('/');

        if(starter.split('/').length === 1) yield *this.searchOnlyCollection(starter)
        else yield *this.searchField(starter, pattern)

        const eventIds = new Set<string>()

        if(listen) for await (const event of this.listen(pattern)) {

            if(event.action !== action && eventIds.has(event.id))  {
                eventIds.delete(event.id)
            } else if(event.action === action && !eventIds.has(event.id)) {
                eventIds.add(event.id)
                yield { _id: event.id as _ulid, docIndexes: event.docIndexes }
            }
        }
    }

    static async getDocIndexes(collection: string, _id: _ulid) {

        const indexes: string[] = []

        if(await exists(`${this.DSK_DB}/${collection}/${this.DIRECT_DIR}/${_id}`)) {

            const files = await opendir(`${this.DSK_DB}/${collection}/${this.DIRECT_DIR}/${_id}`)

            for await (const file of files) {
                if(!file.isSymbolicLink()) continue
                const data = await readlink(`${this.DSK_DB}/${collection}/${this.DIRECT_DIR}/${_id}/${file.name}`)
                indexes.push(data.replace(`${this.DSK_DB}/`, ''))
            }
        }

        return indexes
    }

    private static async *processPattern(pattern: string) {

        const table = pattern.split('/')[0]

        if(!this.listeners.has(table)) {

            if(!await exists(`${this.DSK_DB}/${table}`)) await mkdir(`${this.DSK_DB}/${table}`, { recursive: true })
            
            this.listeners.set(table, watch(`${this.DSK_DB}/${table}`, { recursive: true }))
        }

        const watcher = this.listeners.get(table)!

        for await (const event of watcher) {

            const path = `${table}/${event.filename}`
            
            if(event.filename && new Bun.Glob(pattern).match(path) && event.eventType !== 'change') {

                const id = path.split('/').pop()! as _ulid

                if(await exists(`${this.DSK_DB}/${path}`)) {
                    yield { id, action: "upsert", docIndexes: await this.getDocIndexes(table, id) }
                } else yield { id, action: "delete", docIndexes: [] }
            }
        }
    }

    static async *listen(pattern: string | string[]) {

        if(Array.isArray(pattern)) {

            for(const p of pattern) {
                for await (const event of this.processPattern(p)) yield event
            }
      
        } else {
            for await (const event of this.processPattern(pattern)) yield event
        }
    }
}