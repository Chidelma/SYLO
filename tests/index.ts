import { Redis } from '../src/adapters/redis'
import ttid from '@delma/ttid'

const redisPub = new Redis()
const redisSub = new Redis()

setTimeout(async () => {
    await redisPub.publish("bun", "insert", ttid.generate())
}, 2000)

setTimeout(async () => {
    await redisPub.publish("bun", "insert", ttid.generate())
}, 3000)

await Bun.sleep(1000)

for await (const data of redisSub.subscribe("bun")) {
    console.log("Received:", data)
}
