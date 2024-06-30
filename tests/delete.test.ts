import { test, expect, describe } from 'bun:test'
import Silo from '../src/Stawrij'
import {  _comment, comments, users, _user } from './data'
import { mkdirSync, rmSync } from 'node:fs'

rmSync(process.env.DATA_PREFIX!, {recursive:true})

mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

describe("NO-SQL", async () => {

    const COMMENTS = 'comments'

    await Silo.bulkPutDocs<_comment>(COMMENTS, comments.slice(0, 25))

    let results = await Silo.findDocs<_comment>(COMMENTS, {}).next(1) as _comment[]

    test("DELETE ONE", async () => {

        const id = results[0]._id!

        await Silo.delDoc(COMMENTS, id)

        results = await Silo.findDocs<_comment>(COMMENTS, {}).next() as _comment[]

        const idx = results.findIndex(com => com._id === id)

        expect(idx).toEqual(-1)

    }, 60 * 60 * 1000)

    test("DELETE CLAUSE", async () => {

        await Silo.delDocs<_comment>(COMMENTS, { $ops: [ { name: { $like: "%et%" } } ] })

        results = await Silo.findDocs<_comment>(COMMENTS, { $ops: [ { name: { $like: "%et%" } } ] }).next() as _comment[]

        expect(results.length).toEqual(0)

    }, 60 * 60 * 1000)

    test("DELETE ALL", async () => {

        await Silo.delDocs<_comment>(COMMENTS, {})

        results = await Silo.findDocs<_comment>(COMMENTS, {}).next() as _comment[]

        expect(results.length).toBe(0)

    }, 60 * 60 * 1000)
})


describe("SQL", async () => {

    const USERS = 'users'

    for(const user of users.slice(0, 25)) {
        const keys = Object.keys(user)
        const values = Object.values(user).map((val: any) => { 
            if(typeof val === 'string') return `'${val}'` 
            else if(typeof val === 'object') return JSON.stringify(val)
            else return val
        })
        await Silo.putDocSQL(`INSERT INTO ${USERS} (${keys.join(',')}) VALUES (${values.join(',')})`)
    }

    let results = await Silo.findDocsSQL<_user>(`SELECT * FROM users`).next(1) as _user[]

    test("DELETE CLAUSE", async () => {

        const name = results[0].name!

        await Silo.delDocsSQL<_user>(`DELETE from users WHERE name = '${name}'`)

        results = await Silo.findDocsSQL<_user>(`SELECT * FROM users WHERE name = '${name}'`).next() as _user[]

        const idx = results.findIndex(com => com.name === name)

        expect(idx).toBe(-1)
    })

    test("DELETE ALL", async () => {

        await Silo.delDocsSQL<_user>(`DELETE from users`)

        results = await Silo.findDocsSQL<_user>(`SELECT * FROM users`).next() as _user[]

        expect(results.length).toBe(0)
    })
})