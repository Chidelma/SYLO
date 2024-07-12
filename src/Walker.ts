import { FileChangeInfo, watch } from "node:fs/promises"
import { Glob } from "bun"
import { mkdirSync } from "node:fs"

export default class {

    static listeners: Map<string, AsyncIterable<FileChangeInfo<string>>> = new Map()

    static readonly DB_PATH = process.env.DATA_PREFIX || `${process.cwd()}/db`

    static search(pattern: string) {
        return Array.from(new Glob(pattern).scanSync({ cwd: this.DB_PATH }))
    }

    private static async *streamPattern(pattern: string) {

        let lowestLatency = 500

        const glob = new Glob(pattern)

        const iter = glob.scan({ cwd: this.DB_PATH })

        while(true) {

            let res: IteratorResult<string | undefined, any>

            const startTime = Date.now()

            res = await Promise.race([
                iter.next(),
                new Promise<IteratorResult<string | undefined, any>>(resolve =>
                    setTimeout(() => resolve({ done: true, value: undefined }), lowestLatency)
                )
            ])

            const elapsed = Date.now() - startTime
            if(elapsed < lowestLatency) lowestLatency = elapsed + 1

            if(res.done) break

            yield res.value
        }
    }

    static async *stream(pattern: string | string[]) {

        if(Array.isArray(pattern)) {

            for(const p of pattern) {

                const iter = this.streamPattern(p)

                do {    

                    const res = await iter.next()

                    if(res.done) break

                    yield res.value
                
                } while(true)
            }

        } else {

            const iter = this.streamPattern(pattern)

            do {    

                const res = await iter.next()

                if(res.done) break

                yield res.value
            
            } while(true)
        }   

    }

    private static async *processPattern(pattern: string) {

        const table = pattern.split('/')[0]

        if(!this.listeners.has(table)) {

            if(!await Bun.file(`${this.DB_PATH}/${table}`).exists()) mkdirSync(`${this.DB_PATH}/${table}`, { recursive: true })
            
            this.listeners.set(table, watch(`${this.DB_PATH}/${table}`, { recursive: true }))
        }

        const watcher = this.listeners.get(table)!

        for await (const event of watcher) {

            const path = `${table}/${event.filename}`
            
            if(event.filename && new Glob(pattern).match(path) && event.eventType !== 'change') {

                const id = path.split('/').pop()!

                yield await Bun.file(`${this.DB_PATH}/${path}`).exists() ? { id, action: "upsert" } :{ id, action: "delete" }
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