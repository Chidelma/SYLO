export function validateCollectionName(collection: string): void {
    if (!/^[a-z0-9][a-z0-9\-]*[a-z0-9]$/.test(collection)) {
        throw new Error('Invalid collection name')
    }
}
