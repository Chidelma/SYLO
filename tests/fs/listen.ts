// import Silo from '../../Stawrij'
// import { SILO, _album, albums } from '../data'
// import { mkdirSync, rmSync } from 'node:fs'


// Silo.configureStorages({})

// rmSync(process.env.DATA_PREFIX!, {recursive:true})
// mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

// const ALBUMS = 'albums'

// Silo.findDocs<_album>(ALBUMS, {}, (doc) => {
//     console.log(doc)
// })

// for(const album of albums.slice(0, 25)) {
//     await Silo.putDoc(SILO, ALBUMS, album)
// }

const counters = [1, 2, 3]

async function* gitGenerator() {
    for(const count of counters) {
        await new Promise(resolve => setTimeout(resolve, 1000))
        yield count
    }
}

for await (const value of gitGenerator()) {
    console.log(value);
}

export {}