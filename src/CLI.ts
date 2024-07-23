#!/usr/bin/env node
import Silo from './Stawrij'

try {

    const SQL = process.argv.slice(1)[0]

    const op = SQL.match(/^((?:SELECT|select)|(?:INSERT|insert)|(?:UPDATE|update)|(?:DELETE|delete)|(?:CREATE|create)|(?:ALTER|alter)|(?:TRUNCATE|truncate)|(?:DROP|drop)|(?:USE|use))/i)

    if(!op) throw new Error("Missing SQL Operation")

    const res = await Silo.executeSQL(SQL)

    switch(op[0].toUpperCase()) {
        case "USE":
            console.log("Successfully changed database")
            break
        case "CREATE":
            console.log("Successfully created schema")
            break
        case "ALTER":   
            console.log("Successfully modified schema")
            break
        case "TRUNCATE":
            console.log("Successfully truncated schema")
            break
        case "DROP":
            console.log("Successfully dropped schema")
            break
        case "SELECT":
            if(SQL.includes('JOIN')) console.log(res)
            else console.log(await (res as _storeCursor<Record<string, any>>).collect())
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
            throw new Error("Invalid Operation: " + op[0])
    }

} catch (e) {
    if(e instanceof Error) console.error(e.message)
}