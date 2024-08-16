import Silo from '../../src/Stawrij'

const start = Date.now()

let count = 0

for await (const data of Silo.findDocs<_tips>('tips', { $limit: 1000, $onlyIds: true }).collect()) {
  console.log(data, Date.now() - start, 'ms', ++count)
}