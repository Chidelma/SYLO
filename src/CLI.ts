#!/usr/bin/env node
import Silo from './Stawrij'

try {

    const SQL = process.argv.slice(1)[0]

    const op = SQL.match(/^(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)/i)

    if(!op) throw new Error("Missing SQL Operation")

    const res = await Silo.executeSQL(SQL)

    switch(op[0]) {
        case "CREATE":
            console.log("Successfully created schema")
            break
        case "ALTER":   
            console.log("Successfully modified schema")
            break
        case "DROP":
            console.log("Successfully dropped schema")
            break
        case "SELECT":
            console.log(await (res as _storeCursor<Record<string, any>>).collect())
            break
        case "INSERT":
            console.log(res as _uuid)
            break
        case "UPDATE":
            console.log(`Successfully updated ${res} document(s)`)
            break
        case "DELETE":
            console.log(`Successfully deleted ${res} document(s)`)
            break
        default:
            throw new Error("Invalid Operation")
    }

} catch (e) {
    if(e instanceof Error) console.log(e.message)
}