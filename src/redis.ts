import { RedisClient } from "bun";
import S3 from "./S3";

export default class Redis {

    private client: RedisClient

    private static LOGGING = process.env.LOGGING

    constructor() {

        this.client = new RedisClient(process.env.REDIS_URL ?? 'redis://localhost:6379', {
            connectionTimeout: Number(process.env.REDIS_CONN_TIMEOUT),
            idleTimeout: Number(process.env.REDIS_IDLE_TIMEOUT),
            autoReconnect: process.env.REDIS_AUTO_CONNECT ? true : undefined,
            maxRetries: Number(process.env.REDIS_MAX_RETRIES),
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

            const streamId = await this.client.send('XADD', [
                S3.getBucketFormat(collection),
                `*`,
                action,
                keyId
            ])

            setTimeout(async () => {
                await this.client.send('XDEL', [S3.getBucketFormat(collection), streamId])
            }, 1000)
        }
    }

    async *subscribe(collection: string) {

        if(!this.client.connected) throw new Error('Redis not connected!')
        
        let id = '$'

        do {

            const res = await this.client.send('XREAD', [
                'BLOCK',
                '0',
                'STREAMS',
                S3.getBucketFormat(collection),
                id
            ])

            if(res) {

                const [ data ] = res[S3.getBucketFormat(collection)]

                const [ streamId, message ] = data

                id = streamId

                const [ action, keyId ] = message

                yield { action, keyId }
            }

        } while(true)
    }
}