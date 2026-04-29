/**
 * @param {Record<string, any>} target
 * @param {Record<string, any>} source
 * @returns {Record<string, any>}
 */
export function appendGroup(target, source) {
    const result = { ...target }
    for (const [sourceId, sourceGroup] of Object.entries(source)) {
        if (!result[sourceId]) {
            result[sourceId] = sourceGroup
            continue
        }
        for (const [groupId, groupDoc] of Object.entries(sourceGroup)) {
            result[sourceId][groupId] = groupDoc
        }
    }
    return result
}

Object.assign(Object, { appendGroup })
