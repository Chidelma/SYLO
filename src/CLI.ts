#!/usr/bin/env node
import Silo from '.'

const SQL = process.argv[process.argv.length - 1]

const op = SQL.match(/^((?:SELECT|select)|(?:INSERT|insert)|(?:UPDATE|update)|(?:DELETE|delete)|(?:CREATE|create)|(?:DROP|drop))/i)

if(!op) throw new Error("Missing SQL Operation")

const res = await new Silo().executeSQL(SQL)

const cmnd = op.shift()!

switch(cmnd.toUpperCase()) {
    case "CREATE":
        console.log("Successfully created schema")
        break
    case "DROP":
        console.log("Successfully dropped schema")
        break
    case "SELECT":
        if(typeof res === 'object' && !Array.isArray(res)) console.format(res)
        else console.log(res)
        break
    case "INSERT":
        console.log(res as _ttid)
        break
    case "UPDATE":
        console.log(`Successfully updated ${res} document(s)`)
        break
    case "DELETE":
        console.log(`Successfully deleted ${res} document(s)`)
        break
    default:
        throw new Error("Invalid Operation: " + cmnd)
}