import Silo from '../../Stawrij'
import { SILO, _album, albums } from '../data'
import { mkdirSync, rmSync } from 'node:fs'

Silo.configureStorages({})

rmSync(process.env.DATA_PREFIX!, {recursive:true})
mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

const ALBUMS = 'albums'

for await (const doc of Silo.findDocs<_album>(ALBUMS, {})) {
    console.log(doc)
}