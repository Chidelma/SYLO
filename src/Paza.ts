import { _condition, _op, _operand, _storeDelete, _storeInsert, _storeQuery, _storeUpdate } from "./types/query"

export default class {

    static convertSelect<T>(SQL: string) {

        let query: _storeQuery<Partial<T>> = {} as _storeQuery<Partial<T>>

        try {

            SQL = SQL.toLowerCase()

            const selectMatch = SQL.match(/select\s+(.*?)\s+from\s+(\w+)\s*(?:where\s+(.+?))?$/i)

            if(!selectMatch) throw new Error("Invalid SQL SELECT statement")

            const [_, columns, collection, whereClause] = selectMatch

            if(whereClause) query = this.parseWherClause(whereClause)

            query.$collection = collection

            if(columns !== '*') query.$select = columns.split(',').map(col => col.trim()) as Array<keyof T>

        } catch(e) {
            if(e instanceof Error) throw new Error(`Paser.convertSelect -> ${e.message}`)
        }

        return query
    }

    static convertInsert<T>(SQL: string, params?: Map<keyof T, any>) {

        const insert: _storeInsert<T> = {} as _storeInsert<T>

        try {

            SQL = SQL.toLowerCase()

            const insertMatch = SQL.match(/insert\s+into\s+(\w+)\s*\(([^)]+)\)\s+values\s*\(([^)]+)\)/i)

            if(!insertMatch) throw new Error("Invalid SQL INSERT statement")

            const [_, table, cols] = insertMatch

            insert.$collection = table.trim()

            const columns = cols.trim().split(',')

            const values = SQL.split('values')[1].trim().slice(1, -1).split(',')

            if(columns.length !== values.length) throw new Error("Length of Columns and Values don't match")

            for(let i = 0; i < values.length; i++) {
                insert[columns[i] as keyof T] = values[i] === columns[i] ? params?.get(columns[i] as keyof T) : this.parseValue(values[i]) as any
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`Paser.convertInsert -> ${e.message}`)
        }

        return insert
    }

    static convertUpdate<T>(SQL: string, params?: Map<keyof T, any>) {

        const update: _storeUpdate<Partial<T>> = {} as _storeUpdate<Partial<T>>

        try {

            SQL = SQL.toLowerCase()

            const updateMatch = SQL.match(/update\s+(\w+)\s+set\s+(.+?)(?:\s+where\s+(.+))?$/)

            if(!updateMatch) throw new Error("Invalid SQL UPDATE statement")

            const [_, table, setClause, whereClause] = updateMatch

            update.$collection = table.trim()

            const setConditions = setClause.split(',').map((cond) => cond.trim())

            for(let i = 0; i < setConditions.length; i++) {

                const [col, val] = setConditions[i].split('=').map(s => s.trim())

                update[col as keyof T] = val === col ? params?.get(col as keyof T) : this.parseValue(val) as any
            }

            update.$where = whereClause ? this.parseWherClause(whereClause) : {}

        } catch(e) {
            if(e instanceof Error) throw new Error(`Paser.convertUpdate -> ${e.message}`)
        }

        return update
    }

    static convertDelete<T>(SQL: string) {

        let deleteStore: _storeDelete<Partial<T>> = {} as _storeDelete<Partial<T>>

        try {

            SQL = SQL.toLowerCase()

            const deleteMatch = SQL.match(/delete\s+from\s+(\w+)(?:\s+where\s+(.+))?/i)

            if(!deleteMatch) throw new Error("Invalid SQL DELETE statement")

            const [_, table, whereClause] = deleteMatch

            if(whereClause) deleteStore = this.parseWherClause(whereClause)

            deleteStore.$collection = table

        } catch(e) {
            if(e instanceof Error) throw new Error(`Paser.convertDelete -> ${e.message}`)
        }

        return deleteStore
    }

    private static mapConditionToOperand(condition: _condition) {

        const operand: _operand = {}

        switch(condition.operator) {
            case "=":
                operand.$eq = condition.value
                break
            case "!=":
                operand.$ne = condition.value
                break
            case ">":
                operand.$gt = condition.value as number
                break
            case "<":
                operand.$lt = condition.value as number
                break
            case ">=":
                operand.$gte = condition.value as number
                break
            case "<=":
                operand.$lte = condition.value as number
                break
            case "like":
                operand.$like = (condition.value as string).replaceAll('%', '*')
                break
            default:
                throw new Error(`Unsupported SQL operator: ${condition.operator}`)
        }

        return operand
    }

    private static parseSQLCondition(condition: string) {

        const match = condition.match(/(=|!=|>=|<=|>|<|like)/i)

        if(!match) throw new Error(`Unsupported SQL operator in condition ${condition}`)

        const operator = match[0]

        let [column, value] = condition.split(operator).map(s => s.trim())

        return { column, operator, value: this.parseValue(value) }
    }


    private static parseWherClause<T>(whereClause: string) {

        let result: _storeQuery<Partial<T>> = {} as _storeQuery<Partial<T>>

        try {

            const orConditions: Array<_op<T>> = []

            const orGroups = whereClause.split(/\s+or\s+/i)

            orGroups.forEach((orGroup) => {

                const andGroupConditions: _op<T> = {}
                const andConditionsArray = orGroup.split(/\s+and\s+/i).map(cond => cond.trim())

                andConditionsArray.forEach((cond) => {
                    const condition = this.parseSQLCondition(cond)
                    andGroupConditions[condition.column as keyof T] = this.mapConditionToOperand(condition)
                })

                orConditions.push(andGroupConditions)
            })

            result.$ops = orConditions

        } catch(e) {
            if(e instanceof Error) throw new Error(`Paser.parseWherClause -> ${e.message}`)
        }

        return result
    }

    private static parseValue(value: string) {
    
        const num = Number(value) 

        if(!Number.isNaN(num)) return num

        if(value === "true") return true

        if(value === "false") return false

        if(value === 'null') return null
    
        return value.slice(1, -1)
    }
}