import { _op, _storeQuery, _operand, _condition } from "./types/query"

export default class {

    static convert<T, U extends keyof T>(sql: string) {

        const query: _storeQuery<T, U> = {}

        try {

            const lowerSQL = sql.toLowerCase()

            const limitMatch = lowerSQL.match(/limit\s+(\d+)/)

            if(limitMatch) query.$limit = parseInt(limitMatch[1])

            const orderByMatch = lowerSQL.match(/order\s+by\s+(\w+)\s+(asc|desc)/)

            if(orderByMatch) query.$sort = {
                [orderByMatch[1]]: orderByMatch[2]
            } as Partial<Record<keyof Omit<T, U>, 'asc' | 'desc'>>

            const whereMatch = lowerSQL.match(/where\s+(.+?)(?:\s+order\s+by|\s+limit|$)/)

            if(whereMatch) Object.assign(query, this.parseWherClause(whereMatch[1]))

        } catch(e) {
            if(e instanceof Error) throw new Error(`Query.convert -> ${e.message}`)
        }

        return query
    }

    private static parseWherClause<T, U extends keyof T>(whereClause: string) {

        let result: { $and?: _op<Omit<T, U>>, $or?: Array<_op<Omit<T, U>>> } = {}

        try {

            const andConditions: _op<Omit<T, U>> = {}

            const orConditions: Array<_op<Omit<T, U>>> = {} as Array<_op<Omit<T, U>>>

            const orGroups = whereClause.split(/\s+or\s+/i)

            orGroups.forEach((orGroup) => {

                const andGroupConditions: _op<Omit<T, U>> = {}
                const andConditionsArray = orGroup.split(/\s+and\s+/i).map(cond => cond.trim())

                andConditionsArray.forEach((cond) => {
                    const condition = this.parseSQLCondition(cond)
                    andGroupConditions[condition.column as keyof Omit<T, U>] = this.mapConditionToOperand(condition)
                })

                if(orGroups.length > 1) orConditions.push(andGroupConditions)
                else Object.assign(andConditions, andGroupConditions)
            })

            if(Object.keys(andConditions).length) result.$and = andConditions
            if(orConditions.length) result.$or = orConditions

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

        const operators = ["=", "!=", ">", "<", ">=", "<=", "like"]

        const operator = operators.find((op) => condition.includes(op))

        if(!operator) throw new Error(`Unsupported SQL operator in condition ${condition}`)

        let [column, value] = condition.split(operator).map(s => s.trim())

        return { column, operator, value: this.parseValue(value) }
    }

    static async getExprs<T, U extends keyof T>(collection: string, query: _storeQuery<T, U>) {

        let exprs = new Set<string>()

        try {

            if(query.$and) exprs = new Set([...exprs, ...await this.createAndExp(collection, query.$and)])
            if(query.$or) exprs = new Set([...exprs, ...await this.createOrExp(collection, query.$or)])
            if(query.$nor) exprs = new Set([...exprs, ...await this.createNorExp(collection, query.$nor)])

            const keys: string[] = Object.keys(query).filter((key) => !key.includes('$'))

            const eqVals: Record<keyof Omit<T, U>, string | number| boolean | null> = {} as Record<keyof Omit<T, U>, string | number| boolean | null>

            for(const col of keys) {

                const val = query[col as keyof Omit<T, U>]

                if(typeof val !== "string" && typeof val !== "boolean" && typeof val !== "number" && val !== null && typeof val !== "undefined") {

                    const op = val as Omit<_operand, "$eq">

                    const prefix = `{${collection}}/{${col}}`

                    if(op.$gt) {
                        const valOp = this.getGtOp(String(op.$gt).split('').map((n) => Number(n)))
                        exprs = new Set([...exprs, `${prefix}/${valOp}/**/*`])
                    }
                    if(op.$gte) {
                        const valOp = this.getGteOp(String(op.$gte).split('').map((n) => Number(n)))
                        exprs = new Set([...exprs, `${prefix}/${valOp}/**/*`])
                    }
                    if(op.$lt) {
                        const valOp = this.getLtOp(String(op.$lt).split('').map((n) => Number(n)))
                        exprs = new Set([...exprs, `${prefix}/${valOp}/**/*`])
                    }
                    if(op.$lte) {
                        const valOp = this.getLteOp(String(op.$lte).split('').map((n) => Number(n)))
                        exprs = new Set([...exprs, `${prefix}/${valOp}/**/*`])
                    }
                    if(op.$like) exprs = new Set([...exprs, `${prefix}/${op.$like}/**/*`])

                } else eqVals[col as keyof Omit<T, U>] = val as string | boolean | number | null
            }

            const vals = Object.keys(eqVals)

            if(vals.length > 0) {
                const queryKeys = vals.map((val) => val).join(',')
                const queryVals = Object.values(eqVals).map((val) => val).join(',')
                exprs = new Set([...exprs, `${collection}/{${queryKeys}}/{${queryVals}}/**/*`])
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

    private static async createAndExp<T>(collection: string, ops: _op<T>) {

        let globExprs: string[] = []

        try {

            const prefix = `${collection}/{${Object.keys(ops).join(',')}}`

            const valExp: string[] = []

            for(const col in ops) {

                if(ops[col]!.$eq) valExp.push(ops[col]!.$eq)
                if(ops[col]!.$gt) valExp.push(this.getGtOp(String(ops[col]!.$gt).split('').map((n) => Number(n))))
                if(ops[col]!.$gte) valExp.push(this.getGteOp(String(ops[col]!.$gte).split('').map((n) => Number(n))))
                if(ops[col]!.$lt) valExp.push(this.getLtOp(String(ops[col]!.$lt).split('').map((n) => Number(n))))
                if(ops[col]!.$lte) valExp.push(this.getLteOp(String(ops[col]!.$lte).split('').map((n) => Number(n))))
                if(ops[col]!.$ne) valExp.push(`!(${ops[col]!.$ne})`)
                if(ops[col]!.$like) valExp.push(ops[col]!.$like!)
            }

            globExprs.push(`${prefix}/{${valExp.join(',')}}/**/*`)

        } catch(e) {
            if(e instanceof Error) throw new Error(`Query.createAndExp -> ${e.message}`)
        }

        return globExprs
    }

    private static async createOrExp<T>(collection: string, ops: _op<T>[]) {

        let globExprs: string[] = []

        try {

            for(const op of ops) {

                const prefix = `${collection}/{${Object.keys(op).join(',')}}`

                const valExp: string[] = []

                for(const col in op) {

                    if(op[col]!.$eq) valExp.push(op[col]!.$eq)
                    if(op[col]!.$gt) valExp.push(this.getGtOp(String(op[col]!.$gt).split('').map((n) => Number(n))))
                    if(op[col]!.$gte) valExp.push(this.getGteOp(String(op[col]!.$gte).split('').map((n) => Number(n))))
                    if(op[col]!.$lt) valExp.push(this.getLtOp(String(op[col]!.$lt).split('').map((n) => Number(n))))
                    if(op[col]!.$lte) valExp.push(this.getLteOp(String(op[col]!.$lte).split('').map((n) => Number(n))))
                    if(op[col]!.$ne) valExp.push(`!(${op[col]!.$ne})`)
                    if(op[col]!.$like) valExp.push(op[col]!.$like!)
                }

                globExprs.push(`${prefix}/{${valExp.join(',')}}/**/*`)
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`Query.createOrExp -> ${e.message}`)
        }

        return globExprs
    }

    private static async createNorExp<T>(collection: string, ops: _op<T>[]) {

        let globExprs: string[] = []

        try {

            for(const op of ops) {

                const prefix = `${collection}/{${Object.keys(op).join(',')}}`

                const valExp: string[] = []

                for(const col in op) {

                    if(op[col]!.$eq) valExp.push(`!(${op[col]!.$eq})`)
                    if(op[col]!.$gt) valExp.push(this.getGtOp(String(op[col]!.$gt).split('').map((n) => Number(n)), true))
                    if(op[col]!.$gte) valExp.push(this.getGteOp(String(op[col]!.$gte).split('').map((n) => Number(n))))
                    if(op[col]!.$lt) valExp.push(this.getLtOp(String(op[col]!.$lt).split('').map((n) => Number(n))))
                    if(op[col]!.$lte) valExp.push(this.getLteOp(String(op[col]!.$lte).split('').map((n) => Number(n))))
                    if(op[col]!.$ne) valExp.push(op[col]!.$ne)
                    if(op[col]!.$like) valExp.push(`!(${op[col]!.$like!})`)
                }

                globExprs.push(`${prefix}/{${valExp.join(',')}}/**/*`) 
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`Query.createNorExp -> ${e.message}`)
        }

        return globExprs
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