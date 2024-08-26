import { FileChangeInfo, watch, mkdir, exists } from "node:fs/promises"
import { S3Client, ListObjectsV2Command, _Object } from "@aws-sdk/client-s3"
import ULID from "./ULID"

export default class Walker {

    private static readonly listeners: Map<string, AsyncIterable<FileChangeInfo<string>>> = new Map()

    static readonly DSK_DB = process.env.DB_DIR

    static readonly MEM_DB = process.env.MEM_DIR

    static readonly S3_IDX_DB = process.env.S3_INDEX_BUCKET

    static readonly S3_DATA_DB = process.env.S3_DATA_BUCKET

    private static readonly MAX_KEYS = 1000

    static readonly s3Client = new S3Client({ 
        region: process.env.S3_REGION,
        endpoint: process.env.S3_ENDPOINT
    })

    private static async *searchS3(bucket: string, prefix: string, pattern?: string): AsyncGenerator<{ _id: _ulid, data: string[] } | void, void, { count: number, limit?: number  }> {

        const uniqueIds = new Set<string>()

        let token: string | undefined

        const prefixSegments = prefix.split('/')

        const collection = prefixSegments.shift()!

        let filter = yield

        let limit = filter ? filter.limit : this.MAX_KEYS

        do {

            const res = await this.s3Client.send(new ListObjectsV2Command({
                Bucket: bucket,
                Prefix: prefix,
                MaxKeys: pattern ? limit : undefined,
                ContinuationToken: token,
            }))

            if(res.Contents === undefined) break

            const keys = res.Contents.map(item => item.Key!)

            if(pattern) {

                for(const key of keys) {

                    const segements = key.split('/')

                    const _id = segements.pop()! as _ulid

                    if((ULID.isULID(_id) && !uniqueIds.has(_id)) && new Bun.Glob(pattern).match(key)) {

                        filter = yield { _id, data: await this.getDocData(collection, _id) }

                        limit = filter.limit ? filter.limit : this.MAX_KEYS

                        uniqueIds.add(_id)

                        if(filter.count === limit) break
                    }
                }

            } else {

                const _id = prefix.split('/').pop()! as _ulid

                yield { _id, data: keys }

                break
            }

            token = res.NextContinuationToken

        } while(token !== undefined)
    }

    static async *search(pattern: string, { listen = false, skip = false }: { listen: boolean, skip: boolean }, action: "upsert" | "delete" = "upsert"): AsyncGenerator<{ _id: _ulid, data: string[] } | void, void, { count: number, limit?: number  }> {

        if(!skip) {
            const segments = pattern.split('/');
            const idx = segments.findIndex(seg => seg.includes('*'));
            const prefix = segments.slice(0, idx).join('/');

            yield* this.searchS3(this.S3_IDX_DB!, prefix, pattern)
        }

        const eventIds = new Set<string>()

        if(listen) for await (const event of this.listen(pattern)) {

            if(event.action !== action && eventIds.has(event.id))  {
                eventIds.delete(event.id)
            } else if(event.action === action && !eventIds.has(event.id)) {
                eventIds.add(event.id)
                yield { _id: event.id, data: event.data }
            }
        }
    }

    static async getDocData(collection: string, _id: _ulid) {

        const data: string[] = []

        let finished = false

        const iter = this.searchS3(this.S3_DATA_DB!, `${collection}/${_id}`)

        do {

            const { value, done } = await iter.next()

            if(done) {
                finished = true
                break
            }

            if(value) {
                data.push(...value.data)
                finished = true
                break
            }

        } while(!finished)

        return data
    }

    private static async *processPattern(pattern: string) {

        const table = pattern.split('/').shift()!

        if(!this.listeners.has(table)) {

            if(!await exists(`${this.DSK_DB}/${table}`)) await mkdir(`${this.DSK_DB}/${table}`, { recursive: true })
            
            this.listeners.set(table, watch(`${this.DSK_DB}/${table}`, { recursive: true }))
        }

        const watcher = this.listeners.get(table)!

        const stackIds = new Set<string>()

        for await (const event of watcher) {
            
            if(event.filename && event.eventType === 'rename' && event.filename.split('/').length === 2) {

                const index = event.filename.split('/').pop()!.replaceAll('\\', '/')

                const _id = index.split('/').pop()! as _ulid

                if(ULID.isULID(_id) && new Bun.Glob(pattern).match(index) && !stackIds.has(_id)) {

                    stackIds.add(_id)

                    yield { id: _id, action: "upsert", data: await this.getDocData(table, _id) }

                } else if(!await exists(`${this.DSK_DB}/${table}/.${_id}`)) {

                    yield { id: _id, action: "delete", data: [] }
                
                } else if(ULID.isULID(_id) && stackIds.has(_id)) {

                    stackIds.delete(_id)
                }
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