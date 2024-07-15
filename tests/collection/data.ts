export type _comment = {
    name: string
    email: string
    body: string
}

export type _post = {
    userId: number
    title: string
    body: string
}

export type _album = {
    userId: number
    title: string
}

export type _photo = {
    albumId: number
    title: string
    url: string
    thumbnailUrl: string
}

export type _todo = {
    title: string
    completed: boolean
}

export type _user = {
    name: string
    username: string
    email: string
    address: {
        street: string
        suite: string
        city: string
        zipcode: string
        geo: {
            lat: number
            lng: number
        }
    },
    phone: string
    website: string
    company: {
        name: string
        catchPhrase: string
        bs: string
    }
}

let res = await fetch(`https://jsonplaceholder.typicode.com/comments`)

export const comments: _comment[] = await res.json()

res = await fetch(`https://jsonplaceholder.typicode.com/posts`)

export const posts: _post[] = await res.json()

res = await fetch(`https://jsonplaceholder.typicode.com/albums`)

export const albums: _album[] = await res.json()

res = await fetch(`https://jsonplaceholder.typicode.com/photos`)

export const photos: _photo[] = await res.json()

res = await fetch(`https://jsonplaceholder.typicode.com/todos`)

export const todos: _todo[] = await res.json()

res = await fetch(`https://jsonplaceholder.typicode.com/users`)

export const users: _user[] = await res.json()

export {}