function makeDataUrl(data) {
    return `data:application/json,${encodeURIComponent(JSON.stringify(data))}`
}
function generateAlbums() {
    return Array.from({ length: 100 }, (_, index) => {
        const id = index + 1
        const userId = Math.ceil(id / 10)
        const prefix = id <= 15 ? 'omnis' : id % 4 === 0 ? 'quidem' : 'album'
        return {
            id,
            userId,
            title: `${prefix} album ${id}`
        }
    })
}
function generatePosts() {
    return Array.from({ length: 100 }, (_, index) => {
        const id = index + 1
        const userId = Math.ceil(id / 10)
        return {
            id,
            userId,
            title: `post title ${id}`,
            body: `post body ${id} for user ${userId}`
        }
    })
}
function generateComments() {
    return Array.from({ length: 100 }, (_, index) => {
        const id = index + 1
        return {
            id,
            postId: id,
            name: `comment ${id}`,
            email: `comment${id}@example.com`,
            body: `comment body ${id}`
        }
    })
}
function generatePhotos() {
    return Array.from({ length: 100 }, (_, index) => {
        const id = index + 1
        const title = id % 3 === 0 ? `test photo ${id}` : `photo ${id}`
        return {
            id,
            albumId: Math.ceil(id / 10),
            title,
            url: `https://example.com/photos/${id}.jpg`,
            thumbnailUrl: `https://example.com/photos/${id}-thumb.jpg`
        }
    })
}
function generateTodos() {
    return Array.from({ length: 100 }, (_, index) => {
        const id = index + 1
        return {
            id,
            userId: Math.ceil(id / 10),
            title: id % 4 === 0 ? `test todo ${id}` : `todo ${id}`,
            completed: id % 2 === 0
        }
    })
}
function generateUsers() {
    return Array.from({ length: 10 }, (_, index) => {
        const id = index + 1
        return {
            id,
            name: `User ${id}`,
            username: `user${id}`,
            email: `user${id}@example.com`,
            address: {
                street: `Main Street ${id}`,
                suite: `Suite ${id}`,
                city: id <= 5 ? 'South Christy' : 'North Christy',
                zipcode: `0000${id}`,
                geo: {
                    lat: 10 + id,
                    lng: -20 - id
                }
            },
            phone: `555-000-${String(id).padStart(4, '0')}`,
            website: `user${id}.example.com`,
            company: {
                name: id <= 5 ? 'Acme Labs' : 'Northwind Labs',
                catchPhrase: `Catch phrase ${id}`,
                bs: `business ${id}`
            }
        }
    })
}
export const albumURL = makeDataUrl(generateAlbums())
export const postsURL = makeDataUrl(generatePosts())
export const commentsURL = makeDataUrl(generateComments())
export const photosURL = makeDataUrl(generatePhotos())
export const todosURL = makeDataUrl(generateTodos())
export const usersURL = makeDataUrl(generateUsers())
