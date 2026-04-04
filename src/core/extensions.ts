/* eslint-disable @typescript-eslint/no-explicit-any */

Object.appendGroup = function (target: Record<string, any>, source: Record<string, any>): Record<string, any> {
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
