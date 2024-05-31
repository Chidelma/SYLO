import { _op, _storeQuery, _operand, _condition, _storeUpdate, _storeDelete, _storeInsert } from "./types/query"
import { _schema } from './types/schema'

export default class {

    static getJSONPositions(input: string) {

        const stack: number[] = []
        const squareStack: number[] = []
        const result: number[][] = []

        for (let i = 0; i < input.length; i++) {
            if (input[i] === '{') {
                if (stack.length === 0) stack.push(i);
                else stack.push(i)
            } else if (input[i] === '}') {
                if (stack.length === 0) throw new Error(`Unmatched closing brace at position ${i}`);
                const openingIndex = stack.pop();
                if (stack.length === 0) {
                    if (openingIndex !== undefined) {
                        result.push([openingIndex, i]);
                    }
                }
            } else if (input[i] === '[') {
                if (squareStack.length === 0) squareStack.push(i);
                else squareStack.push(i)
            } else if (input[i] === ']') {
                if (squareStack.length === 0) throw new Error(`Unmatched closing bracket at position ${i}`);
                const openingIndex = squareStack.pop();
                if (squareStack.length === 0) {
                    if (openingIndex !== undefined) {
                        result.push([openingIndex, i]);
                    }
                }
            }
        }

        if (stack.length > 0) {
            throw new Error('Unmatched opening brace(s) found')
        }

        return result
    }

    static convertInsert<T extends _schema<T>>(sql: string) {

        const insert: _storeInsert<T> = {} as _storeInsert<T>

        try {

            const jsonPlaceholder = "_JSON_"

            const lowerSQL = sql.toLowerCase()

            const insertMatch = lowerSQL.match(/insert\s+into\s+(\w+)\s*\((.+?)\)\s*values\s*\((.*?)\)/is)

            if(!insertMatch) throw new Error("Invalid SQL INSERT statement")

            const [_, collection, columString, __] = insertMatch

            const valuesString = lowerSQL.split('values')[1].trim().slice(1, -1)

            insert.$collection = collection

            const values: any[] = []

            const indexes = this.getJSONPositions(valuesString)

            valuesString.split(',').forEach(val => {

                if(!val.includes('\"')) {
                    values.push(val)
                } else values.push(jsonPlaceholder)
            })

            while(indexes.length > 0) {

                const firstIdx = values.indexOf(jsonPlaceholder)
                const lastIndex = values.lastIndexOf(jsonPlaceholder)

                if(firstIdx > -1) {
                    const [start, end] = indexes.shift()!
                    values[firstIdx] = JSON.parse(valuesString.slice(start, end + 1))
                }

                if(indexes.length === 0) break

                if(lastIndex > -1) {
                    const [start, end] = indexes.pop()!
                    values[lastIndex] = JSON.parse(valuesString.slice(start, end + 1))
                }
            }

            const allVaues = values.filter(val => val !== jsonPlaceholder)

            columString.split(',').forEach((col, idx) => {
                const value = allVaues[idx]
                insert[col as keyof Omit<T, '_id'>] = typeof value !== "object" ? this.parseValue(value) as any : value
            })

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

            if(whereClause) query = this.parseWherClause(whereClause)

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

            const updateMatch = lowerSQL.match(/update\s+(\w+)\s+set\s+(.+?)(?:\s+where\s+(.+))?$/)

            if(!updateMatch) throw new Error("Invalid SQL Update Statement")

            const [_, collection, setClause, whereClause] = updateMatch

            update.$collection = collection

            const setConditionsArray = setClause.split(',').map((cond) => cond.trim())

            setConditionsArray.forEach((cond) => {
                const [column, value] = cond.split('=').map((s) => s.trim())
                update[column as keyof Omit<T, '_id'>] = this.parseValue(value) as any
            })

            update.$where = whereClause ? this.parseWherClause(whereClause) : {}

        } catch(e) {
            if(e instanceof Error) throw new Error(`Query.convertUpdate -> ${e.message}`)
        }

        return update
    }

    static convertDelete<T extends _schema<T>>(sql: string) {

        let deleteStore: _storeDelete<T> = {} as _storeDelete<T>

        try {

            const lowerSQL = sql.toLowerCase()

            const whereMatch = lowerSQL.match(/delete\s+from\s+(\w+)(?:\s+where\s+(.+))?/i)

            if(!whereMatch) throw new Error("Invalid SQL DELETE Statement")

            const [_, collection, whereClause] = whereMatch

            if(whereClause) deleteStore = this.parseWherClause(whereClause)

            deleteStore.$collection = collection

        } catch(e) {
            if(e instanceof Error) throw new Error(`Query.convertDelete -> ${e.message}`)
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

    static async getExprs<T>(query: _storeQuery<T>, collection?: string) {

        let exprs = new Set<string>()

        try {

            if(query.$ops) {

                for(const op of query.$ops) {

                    for(const column in op) {

                        const col = op[column as keyof Omit<T, '_id'>]!

                        const prefix = `${collection ?? query.$collection}/${column}`

                        if(col.$eq) exprs = new Set([...exprs, `${prefix}/${col.$eq}/**/*`])
                        if(col.$ne) exprs = new Set([...exprs, `${prefix}/!(${col.$ne})/**/*`])
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
                        if(col.$like) exprs = new Set([...exprs, `${prefix}/${col.$like.replaceAll('%', '*')}/**/*`])
                    }
                }
            } else exprs = new Set([`${collection ?? query.$collection}/**/*`])

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