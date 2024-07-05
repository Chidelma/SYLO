import { test, expect, describe } from 'bun:test'
import Silo from '../src/Stawrij'
import {  _comment, comments, users, _user } from './data'
import { mkdirSync, rmSync } from 'node:fs'
import { _storeCursor, _uuid } from '../src/types/schema'

//rmSync(process.env.DATA_PREFIX!, {recursive:true})

//mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

describe("NO-SQL", async () => {

    const COMMENTS = 'comments'

    await Silo.bulkPutDocs<_comment>(COMMENTS, comments.slice(0, 25))

    let results = await Silo.findDocs<_comment>(COMMENTS, {}).next(1) as Map<_uuid, _comment>

    test("DELETE ONE", async () => {

        const id = Array.from(results.keys())[0]

        await Silo.delDoc(COMMENTS, id)

        results = await Silo.findDocs<_comment>(COMMENTS, {}).next() as Map<_uuid, _comment>

        const idx = Object.keys(results).findIndex(_id => _id === id)

        expect(idx).toEqual(-1)

    }, 60 * 60 * 1000)

    test("DELETE CLAUSE", async () => {

        await Silo.delDocs<_comment>(COMMENTS, { $ops: [ { name: { $like: "%et%" } } ] })

        results = await Silo.findDocs<_comment>(COMMENTS, { $ops: [ { name: { $like: "%et%" } } ] }).next() as Map<_uuid, _comment>

        expect(results.size).toEqual(0)

    }, 60 * 60 * 1000)

    test("DELETE ALL", async () => {

        await Silo.delDocs<_comment>(COMMENTS, {})

        results = await Silo.findDocs<_comment>(COMMENTS, {}).next() as Map<_uuid, _comment>

        expect(results.size).toBe(0)

    }, 60 * 60 * 1000)
})


describe("SQL", async () => {

    const USERS = 'users'

    for(const user of users.slice(0, 25)) {

        const keys = Object.keys(user)
            
        const params: Map<keyof _user, any> = new Map()
        const values: any[] = []

        Object.values(user).forEach((val, idx) => {
            if(typeof val === 'object') {
                params.set(keys[idx] as keyof _user, val)
                values.push(keys[idx])
            } else if(typeof val === 'string') {
                values.push(`'${val}'`)
            } else values.push(val)
        })

        await Silo.executeSQL<_user>(`INSERT INTO ${USERS} (${keys.join(',')}) VALUES (${values.join(',')})`, params)
    }

    let cursor = await Silo.executeSQL<_user>(`SELECT * FROM users`) as _storeCursor<_user>

    let results = await cursor.next(1) as Map<_uuid, _user>

    test("DELETE CLAUSE", async () => {

        const name = Array.from(results.values())[0].name

        await Silo.executeSQL<_user>(`DELETE from users WHERE name = '${name}'`)

        cursor = await Silo.executeSQL<_user>(`SELECT * FROM users WHERE name = '${name}'`) as _storeCursor<_user>

        results = await cursor.next() as Map<_uuid, _user>
        
        const idx = Object.values(results).findIndex(com => com.name === name)

        expect(idx).toBe(-1)
    })

    test("DELETE ALL", async () => {

        await Silo.executeSQL<_user>(`DELETE from users`)

        cursor = await Silo.executeSQL<_user>(`SELECT * FROM users`) as _storeCursor<_user>

        results = await cursor.next() as Map<_uuid, _user>

        expect(results.size).toBe(0)
    })
})