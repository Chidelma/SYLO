/**
 * No-op Redis mock. All methods are silent no-ops so tests never need a
 * running Redis instance. subscribe yields nothing so listener code paths
 * simply exit immediately.
 */
export default class RedisMock {

    async publish(_collection: string, _action: 'insert' | 'delete', _keyId: string | _ttid): Promise<void> {}

    async claimTTID(_id: _ttid, _ttlSeconds: number = 10): Promise<boolean> { return true }

    async *subscribe(_collection: string): AsyncGenerator<never, void, unknown> {}
}
