import { _op, _storeQuery, _operand, _condition, _storeUpdate, _storeDelete, _storeInsert } from "./types/query"
import { _schema } from './types/schema'

export default class {

    static convertInsert<T extends _schema<T>>(sql: string) {

        const insert: _storeInsert<T> = {} as _storeInsert<T>

        try {

            const lowerSQL = sql.toLowerCase()

            const insertMatch = lowerSQL.match(/insert\s+into\s+(\w+)\s*\((.+?)\)\s*values\s*\((.+?)\)/i)

            if(!insertMatch) throw new Error("Invalid SQL INSERT statement")

            const [_, collection, columString, valuesString] = insertMatch

            insert.$collection = collection

            const columns = columString.split(',').map(col => col.trim())
            const values = valuesString.split(',').map(val => val.trim())

            if(columns.length !== values.length) throw new Error("Columns and values count do not match")

            columns.forEach((column, idx) => insert[column as keyof Omit<T, '_id'>] = this.parseValue(values[idx]) as any)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Query.convertQuery -> ${e.message}`)
        }

        return insert
    }

    static convertQuery<T extends _schema<T>>(sql: string) {

        let query: _storeQuery<T> = {} as _storeQuery<T>

        try {

            const lowerSQL = sql.toLowerCase()

            const selectMatch = lowerSQL.match(/select\s+(.*?)\s+from\s+(\w+)\s*(?:where\s+(.+?))?(?:\s+order\s+by\s+(.+?))?(?:\s+limit\s+(\d+))?$/i)

            if(!selectMatch) throw new Error("Invalid SQL SELECT statement")

            const [_, columns, collection, whereClause, orderByClause, limitClause] = selectMatch

            query = this.parseWherClause(whereClause)

            query.$collection = collection

            if(columns !== '*') query.$select = columns.split(',').map(col => col.trim()) as Array<keyof T>

            if(limitClause) query.$limit = parseInt(limitClause.trim(), 10)

            const parseOrderByClause = (orderByClause: string) => {

                const sort: Partial<Record<keyof Omit<T, '_id'>, 'asc' | 'desc'>> = {}

                const columns = orderByClause.split(',')

                columns.forEach(column => {
                    const [col, order] = column.trim().split(/\s+/)
                    sort[col as keyof Omit<T, '_id'>] = (order && order.toLowerCase() === 'desc') ? 'desc' : 'asc'
                })

                return sort
            }

            if(orderByClause) query.$sort = parseOrderByClause(orderByClause)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Query.convertQuery -> ${e.message}`)
        }

        return query
    }

    static convertUpdate<T extends _schema<T>>(sql: string) {

        const update: _storeUpdate<T> = {} as _storeUpdate<T>

        try {

            const lowerSQL = sql.toLowerCase()

            const updateMatch = lowerSQL.match(/update\s+(\w+)\s+set(.+?)\s+where\s+(.+)/i)

            if(!updateMatch) throw new Error("Invalid SQL Update Statement")

            update.$collection = updateMatch[1]

            const setClause = updateMatch[2]

            const setConditionsArray = setClause.split(',').map((cond) => cond.trim())

            setConditionsArray.forEach((cond) => {
                const [column, value] = cond.split('=').map((s) => s.trim())
                update[column as keyof Omit<T, '_id'>] = this.parseValue(value) as any
            })

            update.$where = this.parseWherClause(updateMatch[3])

        } catch(e) {
            if(e instanceof Error) throw new Error(`Query.convertUpdate -> ${e.message}`)
        }

        return update
    }

    static convertDelete<T extends _schema<T>>(sql: string) {

        let deleteStore: _storeDelete<T> = {} as _storeDelete<T>

        try {

            const lowerSQL = sql.toLowerCase()

            const whereMatch = lowerSQL.match(/delete\s+from\s+\w+\s+where\s+(.+)/i)

            if(!whereMatch) throw new Error("Invalid SQL DELETE Statement")

            deleteStore = this.parseWherClause(whereMatch[1])

        } catch(e) {
            if(e instanceof Error) throw new Error(`Query.convertUpdate -> ${e.message}`)
        }

        return deleteStore
    }

    private static parseWherClause<T extends _schema<T>>(whereClause: string) {

        let result: _storeQuery<T> = {} as _storeQuery<T>

        try {

            const orConditions: Array<_op<Omit<T, '_id'>>> = []

            const orGroups = whereClause.split(/\s+or\s+/i)

            orGroups.forEach((orGroup) => {

                const andGroupConditions: _op<Omit<T, '_id'>> = {}
                const andConditionsArray = orGroup.split(/\s+and\s+/i).map(cond => cond.trim())

                andConditionsArray.forEach((cond) => {
                    const condition = this.parseSQLCondition(cond)
                    andGroupConditions[condition.column as keyof Omit<T, '_id'>] = this.mapConditionToOperand(condition)
                })

                orConditions.push(andGroupConditions)
            })

            result.$ops = orConditions

        } catch(e) {
            if(e instanceof Error) throw new Error(`Query.parseWherClause -> ${e.message}`)
        }

        return result
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
                operand.$like = condition.value as string
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

    static async getExprs<T, U extends keyof T>(query: _storeQuery<T>, collection?: string) {

        let exprs = new Set<string>()

        try {

            if(query.$ops) {

                for(const op of query.$ops) {

                    for(const column in op) {

                        const col = op[column as keyof Omit<T, '_id'>]!

                        const prefix = `${collection ?? query.$collection}/${column}`

                        if(col.$eq) exprs = new Set([...exprs, `${prefix}/${col.$eq}/**/*`])
                        if(col.$ne) exprs = new Set([...exprs, `${prefix}/!${col.$ne}/**/*`])
                        if(col.$gt) {
                            const valOp = this.getGtOp(String(col.$gt).split('').map((n) => Number(n)))
                            exprs = new Set([...exprs, `${prefix}/${valOp}/**/*`])
                        }
                        if(col.$gte) {
                            const valOp = this.getGteOp(String(col.$gte).split('').map((n) => Number(n)))
                            exprs = new Set([...exprs, `${prefix}/${valOp}/**/*`])
                        }
                        if(col.$lt) {
                            const valOp = this.getLtOp(String(col.$lt).split('').map((n) => Number(n)))
                            exprs = new Set([...exprs, `${prefix}/${valOp}/**/*`])
                        }
                        if(col.$lte) {
                            const valOp = this.getLteOp(String(col.$lte).split('').map((n) => Number(n)))
                            exprs = new Set([...exprs, `${prefix}/${valOp}/**/*`])
                        }
                        if(col.$like) exprs = new Set([...exprs, `${prefix}/${col.$like}/**/*`])
                    }
                }
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`Query.getExprs -> ${e.message}`)
        }

        return Array.from<string>(exprs)
    }

    private static getGtOp(numbers: number[], negate: boolean = false) {

        let expression = ''

        for(const num of numbers) expression += negate ? `[!${num < 9 ? num + 1 : 9}-9]` : `[${num < 9 ? num + 1 : 9}-9]`

        return expression
    }

    private static getGteOp(numbers: number[], negate: boolean = false) {

        let expression = ''

        for(const num of numbers) expression += negate ? `[!${num < 9 ? num : 9}-9]` : `[${num < 9 ? num : 9}-9]`

        return expression
    }

    private static getLtOp(numbers: number[], negate: boolean = false) {

        let expression = ''

        for(const num of numbers) expression += negate ? `[!0-${num < 9 ? num - 1 : 9}]` : `[0-${num < 9 ? num - 1 : 9}]`

        return expression
    }

    private static getLteOp(numbers: number[], negate: boolean = false) {

        let expression = ''

        for(const num of numbers) expression += negate ? `[!0-${num < 9 ? num : 9}]` :  `[0-${num < 9 ? num : 9}]`

        return expression
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