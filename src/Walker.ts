import S3 from "./S3"
import TTID from "@vyckr/ttid"
import Redis from "./redis"

export default class Walker {

    private static readonly MAX_KEYS = 1000

    private static readonly redis = new Redis()

    private static async *searchS3(collection: string, prefix: string, pattern?: string): AsyncGenerator<{ _id: _ttid, data: string[] } | void, void, { count: number, limit?: number  }> {
        
        const uniqueIds = new Set<string>()

        let token: string | undefined

        let filter = yield

        let limit = filter ? filter.limit : this.MAX_KEYS

        do {

            const res = await S3.list(collection, {
                prefix,
                maxKeys: pattern ? limit : undefined,
                continuationToken: token
            })

            if(res.contents === undefined) break

            const keys = res.contents.map(item => item.key!)

            if(pattern) {

                for(const key of keys) {

                    const segements = key.split('/')

                    const _id = segements.pop()! as _ttid

                    if((TTID.isTTID(_id) && !uniqueIds.has(_id)) && new Bun.Glob(pattern).match(key)) {

                        filter = yield { _id, data: await this.getDocData(collection, _id) }

                        limit = filter.limit ? filter.limit : this.MAX_KEYS

                        uniqueIds.add(_id)

                        if(filter.count === limit) break
                    }
                }

            } else {

                const _id = prefix.split('/').pop()! as _ttid

                yield { _id, data: keys }

                break
            }

            token = res.nextContinuationToken

        } while(token !== undefined)
    }

    static async *search(collection: string, pattern: string, { listen = false, skip = false }: { listen: boolean, skip: boolean }, action: "insert" | "delete" = "insert"): AsyncGenerator<{ _id: _ttid, data: string[] } | void, void, { count: number, limit?: number  }> {

        if(!skip) {
            const segments = pattern.split('/');
            const idx = segments.findIndex(seg => seg.includes('*'));
            const prefix = segments.slice(0, idx).join('/');

            yield* this.searchS3(collection, prefix, pattern)
        }

        const eventIds = new Set<string>()

        if(listen) for await (const event of this.listen(collection, pattern)) {

            if(event.action !== action && eventIds.has(event.id))  {
                eventIds.delete(event.id)
            } else if(event.action === action && !eventIds.has(event.id)) {
                eventIds.add(event.id)
                yield { _id: event.id, data: event.data }
            }
        }
    }

    static async getDocData(collection: string, _id: _ttid) {

        _id = _id.split('-')[0]

        const data: string[] = []

        let finished = false

        const iter = this.searchS3(collection, _id)

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

    private static async *processPattern(collection: string, pattern: string) {

        const stackIds = new Set<string>()

        for await (const { action, keyId } of Walker.redis.subscribe(collection)) {
            
            if(action === 'insert' && !TTID.isTTID(keyId) && new Bun.Glob(pattern).match(keyId)) {

                const _id = keyId.split('/').pop()! as _ttid

                if(!stackIds.has(_id)) {

                    stackIds.add(_id)

                    yield { id: _id, action: "insert", data: await this.getDocData(collection, _id) }
                }

            } else if(action === 'delete' && TTID.isTTID(keyId)) {

                yield { id: keyId as _ttid, action: "delete", data: [] }
            
            } else if(TTID.isTTID(keyId) && stackIds.has(keyId)) {

                stackIds.delete(keyId)
            }
        }
    }

    static async *listen(collection: string, pattern: string | string[]) {

        if(Array.isArray(pattern)) {

            for(const p of pattern) {
                for await (const event of this.processPattern(collection, p)) yield event
            }
      
        } else {
            for await (const event of this.processPattern(collection, pattern)) yield event
        }
    }
}