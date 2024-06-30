import Silo from '../src/Stawrij'
import { _album, _post } from './data'
import { mkdirSync, rmSync } from 'node:fs'

// rmSync(process.env.DATA_PREFIX!, {recursive:true})
// mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

// const POSTS = 'posts'
// let count = 0

// for await (const id of Silo.findDocs<_post>(POSTS, {})) {
//     console.log(id, ++count)
// }



// console.log(Object.hasOwn(Silo.putDoc<_post>('posts', {}), Symbol.asyncIterator))