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