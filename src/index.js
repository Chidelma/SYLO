/**
 * Public package entry. Re-exports the {@link Fylo} default class plus the
 * named error types consumers can catch on.
 */
import Fylo from './api/fylo.js'

export { LocalQueue, QueueMessageContext, consume, publish } from './queue/local.js'
export { FyloAuthError } from './security/auth.js'
export { FyloSyncError } from './replication/sync.js'
export default Fylo
