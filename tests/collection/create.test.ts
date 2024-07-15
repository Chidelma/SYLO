import { test, expect, describe } from 'bun:test'
import Silo from '../../src/Stawrij'
import { _album, _post } from './data'
import { mkdirSync, rmdirSync, readdirSync } from 'node:fs'

rmdirSync(process.env.DATA_PREFIX!, {recursive:true})
mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

describe("NO-SQL", () => {

    const POSTS = 'posts'

    test("CREATE", async () => {

        const treeItems: _treeItem<_post>[] = [{ field: 'userId' }, { field: 'title' }, { field: 'body' }]

        await Silo.createSchema<_post>(POSTS, treeItems)

        const fields = readdirSync(`${process.env.DATA_PREFIX}/${POSTS}`)

        const allFieldsExist = fields.every(field => treeItems.map(item => item.field).includes(field as keyof _post))

        expect(allFieldsExist).toBe(true)

    }, 60 * 60 * 1000)
})

describe("SQL", () => {

    const ALBUMS = 'albums'

    test("CREATE", async () => {

        const treeItems: _treeItem<_album>[] = [{ field: 'userId' }, { field: 'title' }]

        await Silo.executeSQL<_album>(`CREATE TABLE ${ALBUMS} (${JSON.stringify(treeItems)})`)

        const fields = readdirSync(`${process.env.DATA_PREFIX}/${ALBUMS}`)

        const allFieldsExist = fields.every(field => treeItems.map(item => item.field).includes(field as keyof _album))

        expect(allFieldsExist).toBe(true)

    }, 60 * 60 * 1000)
})