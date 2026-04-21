export type FyloAuthAction =
    | 'collection:create'
    | 'collection:drop'
    | 'collection:inspect'
    | 'collection:rebuild'
    | 'doc:read'
    | 'doc:find'
    | 'doc:create'
    | 'doc:update'
    | 'doc:delete'
    | 'bulk:import'
    | 'bulk:export'
    | 'join:execute'
    | 'sql:execute'

export type FyloAuthContext = {
    subjectId: string
    tenantId?: string
    roles?: string[]
    [key: string]: unknown
}

export type FyloAuthorizeInput = {
    auth: FyloAuthContext
    action: FyloAuthAction
    collection?: string
    collections?: string[]
    docId?: string
    data?: unknown
    query?: unknown
    sql?: string
}

export type FyloAuthPolicy = {
    authorize(input: FyloAuthorizeInput): boolean | Promise<boolean>
}

export class FyloAuthError extends Error {
    readonly action: FyloAuthAction
    readonly collection?: string
    readonly docId?: string

    constructor(input: FyloAuthorizeInput) {
        super(
            `FYLO authorization denied for ${input.action}${
                input.collection ? ` on ${input.collection}` : ''
            }${input.docId ? `/${input.docId}` : ''}`
        )
        this.name = 'FyloAuthError'
        this.action = input.action
        this.collection = input.collection
        this.docId = input.docId
    }
}
