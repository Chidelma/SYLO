const [albumResponse, postResponse, commentResponse, photoResponse, todoResponse, userResponse] = await Promise.all([fetch(`https://jsonplaceholder.typicode.com/albums`), fetch(`https://jsonplaceholder.typicode.com/posts`), fetch(`https://jsonplaceholder.typicode.com/comments`), fetch(`https://jsonplaceholder.typicode.com/photos`), fetch(`https://jsonplaceholder.typicode.com/todos`), fetch(`https://jsonplaceholder.typicode.com/users`)])

export const [albums, posts, comments, photos, todos, users] = await Promise.all([albumResponse.json(), postResponse.json(), commentResponse.json(), photoResponse.json(), todoResponse.json(), userResponse.json()])

export {}