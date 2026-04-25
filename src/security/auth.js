/**
 * @typedef {'collection:create' | 'collection:drop' | 'collection:inspect' | 'collection:rebuild' | 'doc:read' | 'doc:find' | 'doc:create' | 'doc:update' | 'doc:delete' | 'bulk:import' | 'bulk:export' | 'join:execute' | 'sql:execute'} FyloAuthAction
 */

/**
 * @typedef {object} FyloAuthContext
 * @property {string} subjectId
 * @property {string=} tenantId
 * @property {string[]=} roles
 * @property {unknown=} [key]
 */

/**
 * @typedef {object} FyloAuthorizeInput
 * @property {FyloAuthContext} auth
 * @property {FyloAuthAction} action
 * @property {string=} collection
 * @property {string[]=} collections
 * @property {string=} docId
 * @property {unknown=} data
 * @property {unknown=} query
 * @property {string=} sql
 */

/**
 * @typedef {object} FyloAuthPolicy
 * @property {(input: FyloAuthorizeInput) => boolean | Promise<boolean>} authorize
 */

export class FyloAuthError extends Error {
    /** @type {FyloAuthAction} */
    action
    /** @type {string | undefined} */
    collection
    /** @type {string | undefined} */
    docId

    /**
     * @param {FyloAuthorizeInput} input
     */
    constructor(input) {
        super(
            `FYLO authorization denied for ${input.action}${input.collection ? ` on ${input.collection}` : ''}${input.docId ? `/${input.docId}` : ''}`
        )
        this.name = 'FyloAuthError'
        this.action = input.action
        this.collection = input.collection
        this.docId = input.docId
    }
}
