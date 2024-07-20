import Silo from '../../src/Stawrij'

const ALBUMS = 'albums'

for await(const data of Silo.findDocs(ALBUMS)) {
    console.log(data)
}