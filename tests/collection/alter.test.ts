import { test, expect, describe } from 'bun:test'
import Silo from '../../src/Stawrij'
import { _photo, _todo, photos } from './data'
import { mkdirSync, rmSync } from 'node:fs' 

rmSync(process.env.DATA_PREFIX!, {recursive:true})
mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

describe("NO-SQL", async () => {

    const PHOTOS = 'photos'

    const treeItems: _treeItem<_photo>[] = [{ field: "albumId" }, { field: "thumbnailUrl" }, { field: "title" }, { field: "url" }]

    await Silo.createSchema<_photo>(PHOTOS, treeItems)

    await Silo.bulkPutDocs<_photo>(PHOTOS, photos.slice(0, 25))

    test("ADD", async () => {

        const ids = await Silo.findDocs<_photo>(PHOTOS, { $limit: 1, $onlyIds: true }).collect() as _uuid[]

        await Silo.patchDoc<_photo>(PHOTOS, new Map([[ids[0], { title: "All Mighty" }]]))

        const results = await Silo.findDocs<_photo>(PHOTOS, { $ops: [{ title: { $eq: "All Mighty" } }]}).collect() as Map<_uuid, _photo>
        
        expect(results.size).toBe(1)
    })

    test("RENAME", async () => {

        const count = await Silo.patchDocs<_photo>(PHOTOS, { title: "All Mighti", $where: { $ops: [{ title: { $like: "%est%" } }] } })

        const results = await Silo.findDocs<_photo>(PHOTOS, { $ops: [ { title: { $eq: "All Mighti" } } ] }).collect() as Map<_uuid, _photo>
        
        expect(results.size).toBe(count)
    })

    test("DROP", async () => {

        const count = await Silo.patchDocs<_photo>(PHOTOS, { title: "All Mighter", $where: {} })

        const results = await Silo.findDocs<_photo>(PHOTOS, { $ops: [ { title: { $eq: "All Mighter" } } ] }).collect() as Map<_uuid, _photo>
        
        expect(results.size).toBe(count)
        
    }, 60 * 60 * 1000)
})