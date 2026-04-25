/**
 * @param {string} collection
 */
export function validateCollectionName(collection) {
    if (!/^[a-z0-9][a-z0-9\-]*[a-z0-9]$/.test(collection)) {
        throw new Error('Invalid collection name')
    }
}
