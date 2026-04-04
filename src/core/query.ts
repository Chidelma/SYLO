export class Query {

    static getExprs<T extends Record<string, any>>(query: _storeQuery<T>) {

        let exprs = new Set<string>()

        if(query.$ops) {

            for(const op of query.$ops) {

                for(const column in op) {

                    const col = op[column as keyof T]!

                    if(col.$eq) exprs.add(`${column}/${col.$eq}/**/*`)
                    if(col.$ne) exprs.add(`${column}/**/*`)
                    if(col.$gt) exprs.add(`${column}/**/*`)
                    if(col.$gte) exprs.add(`${column}/**/*`)
                    if(col.$lt) exprs.add(`${column}/**/*`)
                    if(col.$lte) exprs.add(`${column}/**/*`)
                    if(col.$like) exprs.add(`${column}/${col.$like.replaceAll('%', '*')}/**/*`)
                    if(col.$contains !== undefined) exprs.add(`${column}/*/${String(col.$contains).split('/').join('%2F')}/**/*`)
                }
            }

        } else exprs = new Set([`**/*`])

        return Array.from(exprs)
    }
}
