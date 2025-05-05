export default class {

    static convertTableCRUD(SQL: string) {
        
        const crud: { collection: string } = {} as { collection: string }
        
        try {
            
            const createMatch = SQL.match(/(?:(?:create|CREATE)|(?:alter|ALTER)|(?:truncate|TRUNCATE)|(?:drop|DROP))\s+(?:table|TABLE)\s+([\w-]+)/i)
            
            if(!createMatch) throw new Error("Invalid SQL CREATE statement")
            
            const [_, table] = createMatch

            crud.collection = table.trim()

        } catch(e) {
            if(e instanceof Error) throw new Error(`Parser.convertCreate -> ${e.message}`)
        }
        
        return crud
    }

    static convertSelect<T extends Record<string, any>, U extends Record<string, any> = {}>(SQL: string) {

        let query: _storeQuery<Partial<T>> = {} as _storeQuery<Partial<T>>

        let join: _join<T, U> = {} as _join<T, U>

        try {

            const selectMatch = SQL.match(/(?:select|SELECT)\s+(.*?)\s+(?:from|FROM)\s+([\w-]+)(?:\s+(?:((?:INNER|inner)|(?:LEFT|left)|(?:right|RIGHT)|(?:OUTER|outer))\s+)?(?:join|JOIN)\s+([\w-]+)\s+(?:on|ON)\s+(.+?))?\s*(?:(?:where|WHERE)\s+(.+?))?(?:\s+(?:group by|GROUP BY)\s+([\w-]+))?(?:\s+(?:limit|LIMIT)\s+(\d+))?$/i)

            if(!selectMatch) throw new Error("Invalid SQL SELECT statement")
            
            const [_, columns, collection, mode, joinCollection, joinCondition, whereClause, groupBy, limit] = selectMatch

            if(joinCollection && joinCondition) {

                join = this.parseJoinClause<T, U>(joinCondition)

                join.$leftCollection = collection
                join.$rightCollection = joinCollection

                if(limit) join.$limit = Number(limit)

                if(groupBy) join.$groupby = groupBy.trim() as keyof T

                join.$mode = mode ? mode.toLowerCase().trim() as "inner" | "left" | "right" | "outer" : "inner"

                if(columns !== '*') {

                    const selections = columns.split(',').map(col => col.trim())

                    for(const select of selections) {

                        if(select === '_id') {
                            join.$onlyIds = true
                            break
                        }

                        if(select.includes('AS')) {

                            const [col, alias] = select.split('AS').map(s => s.trim())

                            join.$rename = { ...join.$rename, [col]: alias } as Record<keyof T | keyof U, string>
                        
                            join.$select = [...join.$select ?? [], col]

                        } else join.$select = [...join.$select ?? [], select]
                    }
                }

                return join
            }
            
            if(whereClause) query = this.parseWhereClause(whereClause)

            if(limit) query.$limit = Number(limit)

            if(groupBy) query.$groupby = groupBy.trim() as keyof T

            query.$collection = collection

            if(columns !== '*') {

                const selections = columns.split(',').map(col => col.trim())

                for(const select of selections) {

                    if(select === '_id') {
                        query.$onlyIds = true
                        break
                    }

                    if(select.includes('AS')) {

                        const [col, alias] = select.split('AS').map(s => s.trim())

                        query.$select = [...query.$select ?? [], col]

                        query.$rename = { ...query.$rename, [col]: alias } as Record<keyof T, string>

                    } else query.$select = [...query.$select ?? [], select]
                }
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`Parser.convertSelect -> ${e.message}`)
        }

        return query
    }

    static convertInsert<T extends Record<string, any>>(SQL: string) {

        const insert: _storeInsert<T> = {} as _storeInsert<T>

        try {

            const insertMatch = SQL.match(/(?:insert|INSERT)\s+(?:into|INTO)\s+([\w-]+)\s*\(([^)]+)\)\s+(?:values|VALUES)\s*\(([\s\S]*)\)/i)

            if(!insertMatch) throw new Error("Invalid SQL INSERT statement")

            const [_, table, cols, vals] = insertMatch

            insert.$values = {} as Record<keyof T, any>

            insert.$collection = table.trim()

            const columns = cols.trim().split(',')

            const values = vals.trim().split('|')

            if(columns.length !== values.length) throw new Error("Length of Columns and Values don't match")
            
            for(let i = 0; i < columns.length; i++) {

                try {
                    insert.$values[columns[i] as keyof T] = JSON.parse(values[i])
                } catch(e) {
                    insert.$values[columns[i] as keyof T] = this.parseValue(values[i]) as any
                }
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`Parser.convertInsert -> ${e.message}`)
        }

        return insert
    }

    static convertUpdate<T extends Record<string, any>>(SQL: string) {

        const update: _storeUpdate<Partial<T>> = {} as _storeUpdate<Partial<T>>

        try {

            const updateMatch = SQL.match(/(?:update|UPDATE)\s+([\w-]+)\s+(?:set|SET)\s+(.+?)(?:\s+(?:where|WHERE)\s+(.+))?$/i)

            if(!updateMatch) throw new Error("Invalid SQL UPDATE statement")

            const [_, table, setClause, whereClause] = updateMatch

            update.$set = {} as Record<keyof T, any>

            update.$collection = table.trim()

            const setConditions = setClause.split('|').map((cond) => cond.trim())

            for(let i = 0; i < setConditions.length; i++) {

                const [col, val] = setConditions[i].split('=').map(s => s.trim())

                try {
                    update.$set[col as keyof T] = JSON.parse(val)
                } catch(e) {
                    update.$set[col as keyof T] = this.parseValue(val) as any
                }
            }

            update.$where = whereClause ? this.parseWhereClause(whereClause) : {}

        } catch(e) {
            if(e instanceof Error) throw new Error(`Parser.convertUpdate -> ${e.message}`)
        }

        return update
    }

    static convertDelete<T extends Record<string, any>>(SQL: string) {

        let deleteStore: _storeDelete<Partial<T>> = {} as _storeDelete<Partial<T>>

        try {

            const deleteMatch = SQL.match(/(?:delete|DELETE)\s+(?:from|FROM)\s+([\w-]+)(?:\s+(?:where|WHERE)\s+(.+))?/i)

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


    private static parseJoinClause<T extends Record<string, any>, U extends Record<string, any>>(joinClause: string) {

        let result: _join<T, U> = {} as _join<T, U>

        try {

            const andGroups = joinClause.split(/\s+(?:and|AND)\s+/i).map(cond => cond.trim())

            result.$on = {} as Record<keyof T, _joinOperand<U>>

            for(const cond of andGroups) {

                const condition = this.parseSQLCondition(cond)

                result.$on[condition.column as keyof T] = this.mapConditionToOperand(condition) as _joinOperand<U>
            }

        } catch(e) {
            if(e instanceof Error) throw new Error(`Parser.parseJoinClause -> ${e.message}`)
        }

        return result
    }


    private static parseWhereClause<T extends Record<string, any>>(whereClause: string) {

        let result: _storeQuery<Partial<T>> = {} as _storeQuery<Partial<T>>

        try {

            const orConditions: Array<_op<T>> = []

            const orGroups = whereClause.split(/\s+(?:or|OR)\s+/i)

            orGroups.forEach((orGroup) => {

                const andGroupConditions: _op<T> = {}
                const andConditionsArray = orGroup.split(/\s+(?:and|AND)\s+/i).map(cond => cond.trim())

                andConditionsArray.forEach((cond) => {
                    const condition = this.parseSQLCondition(cond)

                    if(condition.column === '_updated' && !result.$updated) {
                        result.$updated ??= {} as _timestamp
                        if(condition.operator === '<' && !result.$updated.$lt) result.$updated.$lt = Number(condition.value)
                        else if(condition.operator === '>' && !result.$updated.$gt) result.$updated.$gt = Number(condition.value)
                        else if(condition.operator === '>=' && !result.$updated.$gte) result.$updated.$gte = Number(condition.value)
                        else if(condition.operator === '<=' && !result.$updated.$lte) result.$updated.$lte = Number(condition.value)
                        else throw new Error("Invalid SQL UPDATED clause")
                    } 
                    else if(condition.column === '_created' && !result.$created) {
                        result.$created ??= {} as _timestamp
                        if(condition.operator === '<' && !result.$created.$lt) result.$created.$lt = Number(condition.value)
                        else if(condition.operator === '>' && !result.$created.$gt) result.$created.$gt = Number(condition.value)
                        else if(condition.operator === '>=' && !result.$created.$gte) result.$created.$gte = Number(condition.value)
                        else if(condition.operator === '<=' && !result.$created.$lte) result.$created.$lte = Number(condition.value)
                        else throw new Error("Invalid SQL CREATED clause")
                    }
                    else andGroupConditions[condition.column as keyof T] = this.mapConditionToOperand(condition)
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

        try {
            return JSON.parse(value)
        } catch(e) {
            return value.length === 2 ? value : value.slice(1, -1)
        }
    }
}