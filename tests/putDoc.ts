import Silo from '../src/Stawrij'
import { posts, _post } from './data'
import { mkdirSync, rmSync } from 'node:fs'

//rmSync(process.env.DATA_PREFIX!, {recursive:true})
//mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

const POSTS = 'posts'

await Silo.bulkPutDocs<_post>(POSTS, posts.slice(0, 25))

// const startTime = Date.now()

// const docs = await Silo.findDocs<_album>(ALBUMS, {}, true).next()

// console.log("Time Elapsed", Date.now() - startTime)

// console.log(docs)

export {}