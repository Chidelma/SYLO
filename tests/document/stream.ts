import { spawn } from 'bun'
import Silo from '../../src/Stawrij'
import { readdir, opendir } from 'fs/promises'
import Walker from '../../src/Walker'
import { ListObjectsV2Command, DeleteObjectsCommand } from '@aws-sdk/client-s3'

let start = Date.now()

let count = 0

for await (const data of Silo.findDocs<_tips>('tips', { $limit: 100 }).collect()) {
  console.log(data, ++count)
}

console.log(Date.now() - start)