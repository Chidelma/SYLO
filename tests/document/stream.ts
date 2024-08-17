import { spawn } from 'bun'
import Silo from '../../src/Stawrij'

let start = Date.now()

let count = 0

for await (const data of Silo.findDocs<_tips>('tips', { $limit: 100, $onlyIds: true }).collect()) {
  console.log(data, Date.now() - start, 'ms', ++count)
  start = Date.now()
}

// const stream = spawn(['find', `${process.env.DB_DIR}/tips`, '-type', 'f', '-empty'], {
//   stdin: 'pipe',
//   stderr: 'pipe'
// })

// for await (const chunk of stream.stdout) {

//   const paths = new TextDecoder().decode(chunk).split('\n')

//   console.log(paths)
// }