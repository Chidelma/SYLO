export default class {

    static convertUse(SQL: string) {

        try {

            const useMatch = SQL.match(/(?:use|USE)\s+(\w+)/i)

            if(!useMatch) throw new Error("Invalid SQL USE statement")

            const [_, database] = useMatch

            process.env.DATA_PREFIX = database

        } catch(e) {
            if(e instanceof Error) throw new Error(`Parser.convertUse -> ${e.message}`)
        }
    }

    static convertTableCRUD(SQL: string) {
        
        const crud: { collection: string } = {} as { collection: string }
        
        try {
            
            const createMatch = SQL.match(/(?:(?:create|CREATE)|(?:alter|ALTER)|(?:truncate|TRUNCATE)|(?:drop|DROP))\s+(?:table|TABLE)\s+(\w+)/i)
            
            if(!createMatch) throw new Error("Invalid SQL CREATE statement")
            
            const [_, table] = createMatch

            crud.collection = table.trim()

        } catch(e) {
            if(e instanceof Error) throw new Error(`Parser.convertCreate -> ${e.message}`)
        }
        
        return crud
    }

    static convertSelect<T>(SQL: string) {

        let query: _storeQuery<Partial<T>> = {} as _storeQuery<Partial<T>>

        try {

            const selectMatch = SQL.match(/(?:select|SELECT)\s+(.*?)\s+(?:from|FROM)\s+(\w+)\s*(?:(?:where|WHERE)\s+(.+?))?(?:\s+(?:limit|LIMIT)\s+(\d+))?$/i)

            if(!selectMatch) throw new Error("Invalid SQL SELECT statement")

            const [_, columns, collection, whereClause, limit] = selectMatch

            if(whereClause) query = this.parseWhereClause(whereClause)

            if(limit) query.$limit = Number(limit)

            query.$collection = collection

            if(columns !== '*') query.$select = columns.split(',').map(col => col.trim()) as Array<keyof T>

            if(query.$select && query.$select.length === 1 && query.$select[0] === '_id') query.$onlyIds = true

        } catch(e) {
            if(e instanceof Error) throw new Error(`Parser.convertSelect -> ${e.message}`)
        }

        return query
    }

    static convertInsert<T>(SQL: string) {

        const insert: _storeInsert<T> = {} as _storeInsert<T>

        try {

            const insertMatch = SQL.match(/(?:insert|INSERT)\s+(?:into|INTO)\s+(\w+)\s*\(([^)]+)\)\s+(?:values|VALUES)\s*\(([\s\S]*)\)/i)

            if(!insertMatch) throw new Error("Invalid SQL INSERT statement")

            const [_, table, cols, vals] = insertMatch

            insert.$collection = table.trim()

            const columns = cols.trim().split(',')

            const values = vals.trim().split('\\')

            if(columns.length !== values.length) throw new Error("Length of Columns and Values don't match")
            
                for(let i = 0; i < columns.length; i++) {

                try {
                    insert[columns[i] as keyof T] = JSON.parse(values[i])
                } catch(e) {
                    insert[columns[i] as keyof T] = this.parseValue(values[i]) as any
                }
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`Parser.convertInsert -> ${e.message}`)
        }

        return insert
    }

    static convertUpdate<T>(SQL: string) {

        const update: _storeUpdate<Partial<T>> = {} as _storeUpdate<Partial<T>>

        try {

            const updateMatch = SQL.match(/(?:update|UPDATE)\s+(\w+)\s+(?:set|SET)\s+(.+?)(?:\s+(?:where|WHERE)\s+(.+))?$/)

            if(!updateMatch) throw new Error("Invalid SQL UPDATE statement")

            const [_, table, setClause, whereClause] = updateMatch

            update.$collection = table.trim()

            const setConditions = setClause.split('\\').map((cond) => cond.trim())

            for(let i = 0; i < setConditions.length; i++) {

                const [col, val] = setConditions[i].split('=').map(s => s.trim())

                try {
                    update[col as keyof T] = JSON.parse(val)
                } catch(e) {
                    update[col as keyof T] = this.parseValue(val) as any
                }
            }

            update.$where = whereClause ? this.parseWhereClause(whereClause) : {}

        } catch(e) {
            if(e instanceof Error) throw new Error(`Parser.convertUpdate -> ${e.message}`)
        }

        return update
    }

    static convertDelete<T>(SQL: string) {

        let deleteStore: _storeDelete<Partial<T>> = {} as _storeDelete<Partial<T>>

        try {

            const deleteMatch = SQL.match(/(?:delete|DELETE)\s+(?:from|FROM)\s+(\w+)(?:\s+(?:where|WHERE)\s+(.+))?/i)

            if(!deleteMatch) throw new Error("Invalid SQL DELETE statement")

            const [_, table, whereClause] = deleteMatch

            if(whereClause) deleteStore = this.parseWhereClause(whereClause)

            deleteStore.$collection = table

        } catch(e) {
            if(e instanceof Error) throw new Error(`Parser.convertDelete -> ${e.message}`)
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
            case "LIKE":
                operand.$like = (condition.value as string).replaceAll('%', '*')
                break
            default:
                throw new Error(`Unsupported SQL operator: ${condition.operator}`)
        }

        return operand
    }

    private static parseSQLCondition(condition: string) {

        const match = condition.match(/(=|!=|>=|<=|>|<|(?:like|LIKE))/i)

        if(!match) throw new Error(`Unsupported SQL operator in condition ${condition}`)

        const operator = match[0]

        let [column, value] = condition.split(operator).map(s => s.trim())

        return { column, operator, value: this.parseValue(value) }
    }


    private static parseWhereClause<T>(whereClause: string) {

        let result: _storeQuery<Partial<T>> = {} as _storeQuery<Partial<T>>

        try {

            const orConditions: Array<_op<T>> = []

            const orGroups = whereClause.split(/\s+(?:or|OR)\s+/i)

            orGroups.forEach((orGroup) => {

                const andGroupConditions: _op<T> = {}
                const andConditionsArray = orGroup.split(/\s+(?:and|AND)\s+/i).map(cond => cond.trim())

                andConditionsArray.forEach((cond) => {
                    const condition = this.parseSQLCondition(cond)
                    andGroupConditions[condition.column as keyof T] = this.mapConditionToOperand(condition)
                })

                orConditions.push(andGroupConditions)
            })

            result.$ops = orConditions

        } catch(e) {
            if(e instanceof Error) throw new Error(`Parser.parseWherClause -> ${e.message}`)
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