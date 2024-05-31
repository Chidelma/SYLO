import { test, expect, describe } from 'bun:test'
import Silo from '../../Stawrij'
import { SILO, _comment, comments, users, _user } from '../data'
import { mkdirSync, rmSync } from 'node:fs'

Silo.configureStorages({})

rmSync(process.env.DATA_PREFIX!, {recursive:true})

mkdirSync(process.env.DATA_PREFIX!, {recursive:true})

describe("NO-SQL", async () => {

    const COMMENTS = 'comments'

    for(const comment of comments.slice(0, 25)) await Silo.putDoc(SILO, COMMENTS, comment)

    let results = await Silo.findDocs<_comment>(COMMENTS, { $limit: 1 })

    test("DELETE ONE", async () => {

        const id = results[0]._id!

        await Silo.delDoc(SILO, COMMENTS, id)

        results = await Silo.findDocs<_comment>(COMMENTS, {})

        const idx = results.findIndex(com => com._id === id)

        expect(idx).toEqual(-1)

    }, 60 * 60 * 1000)

    test("DELETE CLAUSE", async () => {

        await Silo.delDocs<_comment>(SILO, COMMENTS, { $ops: [ { name: { $like: "%et%" } } ] })

        results = await Silo.findDocs<_comment>(COMMENTS, { $ops: [ { name: { $like: "%et%" } } ] })

        expect(results.length).toEqual(0)

    }, 60 * 60 * 1000)

    test("DELETE ALL", async () => {

        await Silo.delDocs<_comment>(SILO, COMMENTS, {})

        results = await Silo.findDocs<_comment>(COMMENTS, {})

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
        await Silo.putDocSQL(SILO, `INSERT INTO ${USERS} (${keys.join(',')}) VALUES (${values.join(',')})`)
    }

    let results = await Silo.findDocsSQL<_user>(`SELECT * FROM users LIMIT 1`)

    test("DELETE CLAUSE", async () => {

        const name = results[0].name!

        await Silo.delDocsSQL<_user>(SILO, `DELETE from users WHERE name = '${name}'`)

        results = await Silo.findDocsSQL<_user>(`SELECT * FROM users WHERE name = '${name}'`)

        const idx = results.findIndex(com => com.name === name)

        expect(idx).toBe(-1)
    })

    test("DELETE ALL", async () => {

        await Silo.delDocsSQL<_user>(SILO, `DELETE from users`)

        results = await Silo.findDocsSQL<_user>(`SELECT * FROM users`)

        expect(results.length).toBe(0)
    })
})