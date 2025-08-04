

// Token types for SQL lexing
enum TokenType {
    CREATE = 'CREATE',
    DROP = 'DROP',
    SELECT = 'SELECT',
    FROM = 'FROM',
    WHERE = 'WHERE',
    INSERT = 'INSERT',
    INTO = 'INTO',
    VALUES = 'VALUES',
    UPDATE = 'UPDATE',
    SET = 'SET',
    DELETE = 'DELETE',
    JOIN = 'JOIN',
    INNER = 'INNER',
    LEFT = 'LEFT',
    RIGHT = 'RIGHT',
    OUTER = 'OUTER',
    ON = 'ON',
    GROUP = 'GROUP',
    BY = 'BY',
    ORDER = 'ORDER',
    LIMIT = 'LIMIT',
    AS = 'AS',
    AND = 'AND',
    OR = 'OR',
    EQUALS = '=',
    NOT_EQUALS = '!=',
    GREATER_THAN = '>',
    LESS_THAN = '<',
    GREATER_EQUAL = '>=',
    LESS_EQUAL = '<=',
    LIKE = 'LIKE',
    IDENTIFIER = 'IDENTIFIER',
    STRING = 'STRING',
    NUMBER = 'NUMBER',
    BOOLEAN = 'BOOLEAN',
    NULL = 'NULL',
    COMMA = ',',
    SEMICOLON = ';',
    LPAREN = '(',
    RPAREN = ')',
    ASTERISK = '*',
    EOF = 'EOF'
}

interface Token {
    type: TokenType
    value: string
    position: number
}

// SQL Lexer
class SQLLexer {
    private input: string
    private position: number = 0
    private current: string | null = null

    constructor(input: string) {
        this.input = input.trim()
        this.current = this.input[0] || null
    }

    private advance(): void {
        this.position++
        this.current = this.position < this.input.length ? this.input[this.position] : null
    }

    private skipWhitespace(): void {
        while (this.current && /\s/.test(this.current)) {
            this.advance()
        }
    }

    private readString(): string {
        let result = ''
        const quote = this.current
        this.advance() // Skip opening quote
        
        while (this.current && this.current !== quote) {
            result += this.current
            this.advance()
        }
        
        if (this.current === quote) {
            this.advance() // Skip closing quote
        }
        
        return result
    }

    private readNumber(): string {
        let result = ''
        while (this.current && /[\d.]/.test(this.current)) {
            result += this.current
            this.advance()
        }
        return result
    }

    private readIdentifier(): string {
        let result = ''
        while (this.current && /[a-zA-Z0-9_]/.test(this.current)) {
            result += this.current
            this.advance()
        }
        return result
    }

    private getKeywordType(word: string): TokenType {
        const keywords: Record<string, TokenType> = {
            'SELECT': TokenType.SELECT,
            'FROM': TokenType.FROM,
            'WHERE': TokenType.WHERE,
            'INSERT': TokenType.INSERT,
            'INTO': TokenType.INTO,
            'VALUES': TokenType.VALUES,
            'UPDATE': TokenType.UPDATE,
            'SET': TokenType.SET,
            'DELETE': TokenType.DELETE,
            'JOIN': TokenType.JOIN,
            'INNER': TokenType.INNER,
            'LEFT': TokenType.LEFT,
            'RIGHT': TokenType.RIGHT,
            'OUTER': TokenType.OUTER,
            'ON': TokenType.ON,
            'GROUP': TokenType.GROUP,
            'BY': TokenType.BY,
            'ORDER': TokenType.ORDER,
            'LIMIT': TokenType.LIMIT,
            'AS': TokenType.AS,
            'AND': TokenType.AND,
            'OR': TokenType.OR,
            'LIKE': TokenType.LIKE,
            'TRUE': TokenType.BOOLEAN,
            'FALSE': TokenType.BOOLEAN,
            'NULL': TokenType.NULL
        }
        return keywords[word.toUpperCase()] || TokenType.IDENTIFIER
    }

    tokenize(): Token[] {
        const tokens: Token[] = []

        while (this.current) {
            this.skipWhitespace()
            
            if (!this.current) break

            const position = this.position

            // String literals
            if (this.current === "'" || this.current === '"') {
                const value = this.readString()
                tokens.push({ type: TokenType.STRING, value, position })
                continue
            }

            // Numbers
            if (/\d/.test(this.current)) {
                const value = this.readNumber()
                tokens.push({ type: TokenType.NUMBER, value, position })
                continue
            }

            // Identifiers and keywords
            if (/[a-zA-Z_]/.test(this.current)) {
                const value = this.readIdentifier()
                const type = this.getKeywordType(value)
                tokens.push({ type, value, position })
                continue
            }

            // Operators and punctuation
            switch (this.current) {
                case '=':
                    tokens.push({ type: TokenType.EQUALS, value: '=', position })
                    this.advance()
                    break
                case '!':
                    if (this.input[this.position + 1] === '=') {
                        tokens.push({ type: TokenType.NOT_EQUALS, value: '!=', position })
                        this.advance()
                        this.advance()
                    } else {
                        this.advance()
                    }
                    break
                case '>':
                    if (this.input[this.position + 1] === '=') {
                        tokens.push({ type: TokenType.GREATER_EQUAL, value: '>=', position })
                        this.advance()
                        this.advance()
                    } else {
                        tokens.push({ type: TokenType.GREATER_THAN, value: '>', position })
                        this.advance()
                    }
                    break
                case '<':
                    if (this.input[this.position + 1] === '=') {
                        tokens.push({ type: TokenType.LESS_EQUAL, value: '<=', position })
                        this.advance()
                        this.advance()
                    } else {
                        tokens.push({ type: TokenType.LESS_THAN, value: '<', position })
                        this.advance()
                    }
                    break
                case ',':
                    tokens.push({ type: TokenType.COMMA, value: ',', position })
                    this.advance()
                    break
                case ';':
                    tokens.push({ type: TokenType.SEMICOLON, value: ';', position })
                    this.advance()
                    break
                case '(':
                    tokens.push({ type: TokenType.LPAREN, value: '(', position })
                    this.advance()
                    break
                case ')':
                    tokens.push({ type: TokenType.RPAREN, value: ')', position })
                    this.advance()
                    break
                case '*':
                    tokens.push({ type: TokenType.ASTERISK, value: '*', position })
                    this.advance()
                    break
                default:
                    this.advance()
                    break
            }
        }

        tokens.push({ type: TokenType.EOF, value: '', position: this.position })
        return tokens
    }
}

// SQL Parser
class SQLParser {
    private tokens: Token[]
    private position: number = 0
    private current: Token

    constructor(tokens: Token[]) {
        this.tokens = tokens
        this.current = tokens[0]
    }

    private advance(): void {
        this.position++
        this.current = this.tokens[this.position] || { type: TokenType.EOF, value: '', position: -1 }
    }

    private expect(type: TokenType): Token {
        if (this.current.type !== type) {
            throw new Error(`Expected ${type}, got ${this.current.type} at position ${this.current.position}`)
        }
        const token = this.current
        this.advance()
        return token
    }

    private match(...types: TokenType[]): boolean {
        return types.includes(this.current.type)
    }

    private parseValue(): any {
        if (this.current.type === TokenType.STRING) {
            const value = this.current.value
            this.advance()
            return value
        }
        if (this.current.type === TokenType.NUMBER) {
            const value = parseFloat(this.current.value)
            this.advance()
            return value
        }
        if (this.current.type === TokenType.BOOLEAN) {
            const value = this.current.value.toLowerCase() === 'true'
            this.advance()
            return value
        }
        if (this.current.type === TokenType.NULL) {
            this.advance()
            return null
        }
        throw new Error(`Unexpected value type: ${this.current.type}`)
    }

    private parseOperator(): string {
        const operatorMap: Partial<Record<TokenType, string>> = {
            [TokenType.EQUALS]: '$eq',
            [TokenType.NOT_EQUALS]: '$ne',
            [TokenType.GREATER_THAN]: '$gt',
            [TokenType.LESS_THAN]: '$lt',
            [TokenType.GREATER_EQUAL]: '$gte',
            [TokenType.LESS_EQUAL]: '$lte',
            [TokenType.LIKE]: '$like'
        }

        if (operatorMap[this.current.type]) {
            const op = operatorMap[this.current.type]
            this.advance()
            return op ?? ''
        }
        
        throw new Error(`Unknown operator: ${this.current.type}`)
    }

    private parseCondition(): _condition {
        const column = this.expect(TokenType.IDENTIFIER).value
        const operator = this.parseOperator()
        const value = this.parseValue()
        
        return { column, operator, value }
    }

    private parseWhereClause<T>(): Array<_op<T>> {
        this.expect(TokenType.WHERE)
        const conditions: Array<_op<T>> = []
        
        do {
            const condition = this.parseCondition()
            const op: _op<T> = {
                [condition.column as keyof T]: {
                    [condition.operator]: condition.value
                } as _operand
            } as _op<T>
            conditions.push(op)
            
            if (this.match(TokenType.AND, TokenType.OR)) {
                this.advance()
            } else {
                break
            }
        } while (true)
        
        return conditions
    }

    private parseSelectClause(): string[] {
        this.expect(TokenType.SELECT)
        const columns: string[] = []
        
        if (this.current.type === TokenType.ASTERISK) {
            this.advance()
            return ['*']
        }
        
        do {
            columns.push(this.expect(TokenType.IDENTIFIER).value)
            if (this.current.type === TokenType.COMMA) {
                this.advance()
            } else {
                break
            }
        } while (true)
        
        return columns
    }

    parseSelect<T extends Record<string, any>>(): _storeQuery<T> | _join<T, any> {
        const select = this.parseSelectClause()
        this.expect(TokenType.FROM)
        const collection = this.expect(TokenType.IDENTIFIER).value
        
        // Check if this is a JOIN query
        if (this.match(TokenType.JOIN, TokenType.INNER, TokenType.LEFT, TokenType.RIGHT, TokenType.OUTER)) {
            return this.parseJoinQuery<T, any>(select, collection)
        }
        
        const query: _storeQuery<T> = {
            $collection: collection,
            $select: select.includes('*') ? undefined : select as Array<keyof T>,
            $onlyIds: select.includes('_id')
        }
        
        if (this.match(TokenType.WHERE)) {
            query.$ops = this.parseWhereClause<T>()
        }
        
        if (this.match(TokenType.GROUP)) {
            this.advance()
            this.expect(TokenType.BY)
            query.$groupby = this.expect(TokenType.IDENTIFIER).value as keyof T
        }
        
        if (this.match(TokenType.LIMIT)) {
            this.advance()
            query.$limit = parseInt(this.expect(TokenType.NUMBER).value)
        }
        
        return query
    }

    parseJoinQuery<T extends Record<string, any>, U extends Record<string, any>>(
        select: string[], 
        leftCollection: string
    ): _join<T, U> {
        // Parse join type
        let joinMode: "inner" | "left" | "right" | "outer" = "inner"
        
        if (this.match(TokenType.INNER)) {
            this.advance()
            joinMode = "inner"
        } else if (this.match(TokenType.LEFT)) {
            this.advance()
            joinMode = "left"
        } else if (this.match(TokenType.RIGHT)) {
            this.advance()
            joinMode = "right"
        } else if (this.match(TokenType.OUTER)) {
            this.advance()
            joinMode = "outer"
        }
        
        this.expect(TokenType.JOIN)
        const rightCollection = this.expect(TokenType.IDENTIFIER).value
        this.expect(TokenType.ON)
        
        // Parse join conditions
        const onConditions = this.parseJoinConditions<T, U>()
        
        const joinQuery: _join<T, U> = {
            $leftCollection: leftCollection,
            $rightCollection: rightCollection,
            $mode: joinMode,
            $on: onConditions,
            $select: select.includes('*') ? undefined : select as Array<keyof T | keyof U>
        }
        
        // Parse additional clauses
        if (this.match(TokenType.WHERE)) {
            // For joins, WHERE conditions would need to be handled differently
            // Skip for now as it's complex with joined tables
            this.parseWhereClause<T>()
        }
        
        if (this.match(TokenType.GROUP)) {
            this.advance()
            this.expect(TokenType.BY)
            joinQuery.$groupby = this.expect(TokenType.IDENTIFIER).value as keyof T | keyof U
        }
        
        if (this.match(TokenType.LIMIT)) {
            this.advance()
            joinQuery.$limit = parseInt(this.expect(TokenType.NUMBER).value)
        }
        
        return joinQuery
    }

    private parseJoinConditions<T, U>(): _on<T, U> {
        const conditions: _on<T, U> = {}
        
        do {
            // Parse: table1.column = table2.column
            const leftSide = this.parseJoinColumn()
            const operator = this.parseJoinOperator()
            const rightSide = this.parseJoinColumn()
            
            // Build the join condition
            const leftColumn = leftSide.column as keyof T
            const rightColumn = rightSide.column as keyof U
            
            if (!conditions[leftColumn]) {
                conditions[leftColumn] = {} as _joinOperand<U>
            }
            
            (conditions[leftColumn] as any)[operator] = rightColumn
            
            if (this.match(TokenType.AND)) {
                this.advance()
            } else {
                break
            }
        } while (true)
        
        return conditions
    }

    private parseJoinColumn(): { table?: string, column: string } {
        const identifier = this.expect(TokenType.IDENTIFIER).value
        
        // Check if it's table.column format
        if (this.current.type === TokenType.IDENTIFIER) {
            // This might be a qualified column name, but we'll treat it as simple for now
            return { column: identifier }
        }
        
        return { column: identifier }
    }

    private parseJoinOperator(): string {
        const operatorMap: Record<string, string> = {
            [TokenType.EQUALS]: '$eq',
            [TokenType.NOT_EQUALS]: '$ne',
            [TokenType.GREATER_THAN]: '$gt',
            [TokenType.LESS_THAN]: '$lt',
            [TokenType.GREATER_EQUAL]: '$gte',
            [TokenType.LESS_EQUAL]: '$lte'
        }

        if (operatorMap[this.current.type]) {
            const op = operatorMap[this.current.type]
            this.advance()
            return op
        }
        
        throw new Error(`Unknown join operator: ${this.current.type}`)
    }

    parseInsert<T extends Record<string, any>>(): _storeInsert<T> {
        this.expect(TokenType.INSERT)
        this.expect(TokenType.INTO)
        const collection = this.expect(TokenType.IDENTIFIER).value
        
        // Parse column list
        let columns: string[] = []
        if (this.current.type === TokenType.LPAREN) {
            this.advance()
            do {
                columns.push(this.expect(TokenType.IDENTIFIER).value)
                // @ts-ignore
                if (this.current.type === TokenType.COMMA) {
                    this.advance()
                } else {
                    break
                }
            } while (true)
            this.expect(TokenType.RPAREN)
        }
        
        this.expect(TokenType.VALUES)
        this.expect(TokenType.LPAREN)
        
        const values: any = {}
        let valueIndex = 0
        
        do {
            const value = this.parseValue()
            const column = columns[valueIndex] || `col${valueIndex}`
            values[column] = value
            valueIndex++
            
            if (this.current.type === TokenType.COMMA) {
                this.advance()
            } else {
                break
            }
        } while (true)
        
        this.expect(TokenType.RPAREN)
        
        return {
            $collection: collection,
            $values: values as { [K in keyof T]: T[K] }
        }
    }

    parseUpdate<T extends Record<string, any>>(): _storeUpdate<T> {
        this.expect(TokenType.UPDATE)
        const collection = this.expect(TokenType.IDENTIFIER).value
        this.expect(TokenType.SET)
        
        const set: any = {}
        
        do {
            const column = this.expect(TokenType.IDENTIFIER).value
            this.expect(TokenType.EQUALS)
            const value = this.parseValue()
            set[column] = value
            
            if (this.current.type === TokenType.COMMA) {
                this.advance()
            } else {
                break
            }
        } while (true)
        
        const update: _storeUpdate<T> = {
            $collection: collection,
            $set: set as { [K in keyof Partial<T>]: T[K] }
        }
        
        if (this.match(TokenType.WHERE)) {
            const whereQuery: _storeQuery<T> = {
                $collection: collection,
                $ops: this.parseWhereClause<T>()
            }
            update.$where = whereQuery
        }
        
        return update
    }

    parseDelete<T extends Record<string, any>>(): _storeDelete<T> {
        this.expect(TokenType.DELETE)
        this.expect(TokenType.FROM)
        const collection = this.expect(TokenType.IDENTIFIER).value
        
        const deleteQuery: _storeDelete<T> = {
            $collection: collection
        }
        
        if (this.match(TokenType.WHERE)) {
            deleteQuery.$ops = this.parseWhereClause<T>()
        }
        
        return deleteQuery
    }
}

// Main SQL to AST converter
export default class {
    static parse<T extends Record<string, any>, U extends Record<string, any> = any>(sql: string): 
        _storeQuery<T> | _storeInsert<T> | _storeUpdate<T> | _storeDelete<T> | _join<T, U> {
        
        const lexer = new SQLLexer(sql)
        const tokens = lexer.tokenize()
        const parser = new SQLParser(tokens)
        
        // Determine query type based on first token
        const firstToken = tokens[0]
        
        switch (firstToken.value) {
            case TokenType.CREATE:
                return { $collection: tokens[2].value }
            case TokenType.SELECT:
                return parser.parseSelect<T>()
            case TokenType.INSERT:
                return parser.parseInsert<T>()
            case TokenType.UPDATE:
                return parser.parseUpdate<T>()
            case TokenType.DELETE:
                return parser.parseDelete<T>()
            case TokenType.DROP:
                return { $collection: tokens[2].value }
            default:
                throw new Error(`Unsupported SQL statement type: ${firstToken.value}`)
        }
    }

    // Bun SQL inspired query builder methods
    static query<T extends Record<string, any>>(collection: string): QueryBuilder<T> {
        return new QueryBuilder<T>(collection)
    }

    // Join query builder
    static join<T extends Record<string, any>, U extends Record<string, any>>(
        leftCollection: string, 
        rightCollection: string
    ): JoinBuilder<T, U> {
        return new JoinBuilder<T, U>(leftCollection, rightCollection)
    }
}

// Bun SQL inspired query builder
export class QueryBuilder<T extends Record<string, any>> {
    private collection: string
    private queryAst: Partial<_storeQuery<T>> = {}

    constructor(collection: string) {
        this.collection = collection
        this.queryAst.$collection = collection
    }

    select(...columns: Array<keyof T>): this {
        this.queryAst.$select = columns
        return this
    }

    where(conditions: Array<_op<T>>): this {
        this.queryAst.$ops = conditions
        return this
    }

    limit(count: number): this {
        this.queryAst.$limit = count
        return this
    }

    groupBy(column: keyof T): this {
        this.queryAst.$groupby = column
        return this
    }

    onlyIds(): this {
        this.queryAst.$onlyIds = true
        return this
    }

    build(): _storeQuery<T> {
        return this.queryAst as _storeQuery<T>
    }

    // Convert to SQL string (reverse operation)
    toSQL(): string {
        let sql = 'SELECT '
        
        if (this.queryAst.$select) {
            sql += this.queryAst.$select.join(', ')
        } else {
            sql += '*'
        }
        
        sql += ` FROM ${this.collection}`
        
        if (this.queryAst.$ops && this.queryAst.$ops.length > 0) {
            sql += ' WHERE '
            const conditions = this.queryAst.$ops.map(op => {
                const entries = Object.entries(op)
                return entries.map(([column, operand]) => {
                    const opEntries = Object.entries(operand as _operand)
                    return opEntries.map(([operator, value]) => {
                        const sqlOp = this.operatorToSQL(operator)
                        const sqlValue = typeof value === 'string' ? `'${value}'` : value
                        return `${column} ${sqlOp} ${sqlValue}`
                    }).join(' AND ')
                }).join(' AND ')
            }).join(' AND ')
            sql += conditions
        }
        
        if (this.queryAst.$groupby) {
            sql += ` GROUP BY ${String(this.queryAst.$groupby)}`
        }
        
        if (this.queryAst.$limit) {
            sql += ` LIMIT ${this.queryAst.$limit}`
        }
        
        return sql
    }

    private operatorToSQL(operator: string): string {
        const opMap: Record<string, string> = {
            '$eq': '=',
            '$ne': '!=',
            '$gt': '>',
            '$lt': '<',
            '$gte': '>=',
            '$lte': '<=',
            '$like': 'LIKE'
        }
        return opMap[operator] || '='
    }
}

// Join query builder
export class JoinBuilder<T extends Record<string, any>, U extends Record<string, any>> {
    private joinAst: Partial<_join<T, U>> = {}

    constructor(leftCollection: string, rightCollection: string) {
        this.joinAst.$leftCollection = leftCollection
        this.joinAst.$rightCollection = rightCollection
        this.joinAst.$mode = 'inner' // default
    }

    select(...columns: Array<keyof T | keyof U>): this {
        this.joinAst.$select = columns
        return this
    }

    innerJoin(): this {
        this.joinAst.$mode = 'inner'
        return this
    }

    leftJoin(): this {
        this.joinAst.$mode = 'left'
        return this
    }

    rightJoin(): this {
        this.joinAst.$mode = 'right'
        return this
    }

    outerJoin(): this {
        this.joinAst.$mode = 'outer'
        return this
    }

    on(conditions: _on<T, U>): this {
        this.joinAst.$on = conditions
        return this
    }

    limit(count: number): this {
        this.joinAst.$limit = count
        return this
    }

    groupBy(column: keyof T | keyof U): this {
        this.joinAst.$groupby = column
        return this
    }

    onlyIds(): this {
        this.joinAst.$onlyIds = true
        return this
    }

    rename(mapping: Record<keyof Partial<T> | keyof Partial<U>, string>): this {
        this.joinAst.$rename = mapping
        return this
    }

    build(): _join<T, U> {
        if (!this.joinAst.$on) {
            throw new Error('JOIN query must have ON conditions')
        }
        return this.joinAst as _join<T, U>
    }

    // Convert to SQL string
    toSQL(): string {
        let sql = 'SELECT '
        
        if (this.joinAst.$select) {
            sql += this.joinAst.$select.join(', ')
        } else {
            sql += '*'
        }
        
        sql += ` FROM ${this.joinAst.$leftCollection}`
        
        // Add join type
        const joinType = this.joinAst.$mode?.toUpperCase() || 'INNER'
        sql += ` ${joinType} JOIN ${this.joinAst.$rightCollection}`
        
        // Add ON conditions
        if (this.joinAst.$on) {
            sql += ' ON '
            const conditions = Object.entries(this.joinAst.$on).map(([leftCol, operand]) => {
                return Object.entries(operand as _joinOperand<U>).map(([operator, rightCol]) => {
                    const sqlOp = this.operatorToSQL(operator)
                    return `${this.joinAst.$leftCollection}.${leftCol} ${sqlOp} ${this.joinAst.$rightCollection}.${String(rightCol)}`
                }).join(' AND ')
            }).join(' AND ')
            sql += conditions
        }
        
        if (this.joinAst.$groupby) {
            sql += ` GROUP BY ${String(this.joinAst.$groupby)}`
        }
        
        if (this.joinAst.$limit) {
            sql += ` LIMIT ${this.joinAst.$limit}`
        }
        
        return sql
    }

    private operatorToSQL(operator: string): string {
        const opMap: Record<string, string> = {
            '$eq': '=',
            '$ne': '!=',
            '$gt': '>',
            '$lt': '<',
            '$gte': '>=',
            '$lte': '<=',
        }
        return opMap[operator] || '='
    }
}