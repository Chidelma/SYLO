import { RedisClient } from "bun";
import { S3 } from "./s3";

export class Redis {

    private client: RedisClient

    private static LOGGING = process.env.LOGGING

    constructor() {

        this.client = new RedisClient(process.env.REDIS_URL ?? 'redis://localhost:6379', {
            connectionTimeout: process.env.REDIS_CONN_TIMEOUT ? Number(process.env.REDIS_CONN_TIMEOUT) : undefined,
            idleTimeout: process.env.REDIS_IDLE_TIMEOUT ? Number(process.env.REDIS_IDLE_TIMEOUT) : undefined,
            autoReconnect: process.env.REDIS_AUTO_CONNECT ? true : undefined,
            maxRetries: process.env.REDIS_MAX_RETRIES ? Number(process.env.REDIS_MAX_RETRIES) : undefined,
            enableOfflineQueue: process.env.REDIS_ENABLE_OFFLINE_QUEUE ? true : undefined,
            enableAutoPipelining: process.env.REDIS_ENABLE_AUTO_PIPELINING ? true : undefined,
            tls: process.env.REDIS_TLS ? true : undefined
        })

        this.client.onconnect = () => {
            if(Redis.LOGGING) console.log("Client Connected")
        }

        this.client.onclose = (err) => console.error("Redis client connection closed", err.message)

        this.client.connect()
    }

    async publish(collection: string, action: 'insert' | 'delete', keyId: string | _ttid) {

        if(this.client.connected) {

            await this.client.publish(S3.getBucketFormat(collection), JSON.stringify({ action, keyId }))
        }
    }

    async claimTTID(_id: _ttid, ttlSeconds: number = 10): Promise<boolean> {

        if(!this.client.connected) return false

        const result = await this.client.send('SET', [`ttid:${_id}`, '1', 'NX', 'EX', String(ttlSeconds)])

        return result === 'OK'
    }

    async *subscribe(collection: string) {

        if(!this.client.connected) throw new Error('Redis not connected!')

        const client = this.client

        const stream = new ReadableStream({
            async start(controller) {
                await client.subscribe(S3.getBucketFormat(collection), (message) => {
                    controller.enqueue(message)
                })
            },
        })

        for await (const chunk of stream) {
            yield JSON.parse(chunk)
        }
    }
}
