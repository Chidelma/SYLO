interface _template {
    name: string
    age: number
    verified: boolean
    address: {
    street_number: number
    street_name: string
    city: string
    province: string | null
    country: string
}
    hobbies: Array<string>
    other_details: Record<string, string>
    location: Array<number>
}
