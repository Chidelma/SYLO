import { mkdtemp } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'

export async function createTestRoot(prefix = 'fylo-test-') {
    return await mkdtemp(path.join(os.tmpdir(), prefix))
}
