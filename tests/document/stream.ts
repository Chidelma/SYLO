import Silo from '../../src/Stawrij'

const PHOTOS = 'photos'

let count = 0

for await(const data of Silo.findDocs(PHOTOS)) {
    console.log(data, ++count)
}