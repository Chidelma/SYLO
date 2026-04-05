import { test, expect, describe, beforeAll, afterAll, mock } from 'bun:test'
import Fylo from '../../src'
import { usersURL } from '../data'
import S3Mock from '../mocks/s3'
import RedisMock from '../mocks/redis'

/**
 * _user has deeply nested fields:
 *   address.street, address.suite, address.city, address.zipcode
 *   address.geo.lat, address.geo.lng
 *   company.name, company.catchPhrase, company.bs
 *
 * This exercises the recursive branch in Dir.extractKeys and the nested
 * path reconstruction in Dir.constructData.
 */

const USERS = 'nst-user'

let insertedCount = 0
let sampleId: _ttid

const fylo = new Fylo()

mock.module('../../src/adapters/s3', () => ({ S3: S3Mock }))
mock.module('../../src/adapters/redis', () => ({ Redis: RedisMock }))

beforeAll(async () => {
    await Fylo.createCollection(USERS)
    try {
        insertedCount = await fylo.importBulkData<_user>(USERS, new URL(usersURL))
    } catch {
        await fylo.rollback()
    }

    for await (const data of Fylo.findDocs<_user>(USERS, { $limit: 1, $onlyIds: true }).collect()) {
        sampleId = data as _ttid
    }
})

afterAll(async () => {
    await Fylo.dropCollection(USERS)
})

describe("NO-SQL", async () => {

    test("SELECT ALL — nested documents are returned", async () => {

        let results: Record<_ttid, _user> = {}

        for await (const data of Fylo.findDocs<_user>(USERS).collect()) {
            results = { ...results, ...data as Record<_ttid, _user> }
        }

        expect(Object.keys(results).length).toBe(insertedCount)
    })

    test("GET ONE — top-level fields are reconstructed correctly", async () => {

        const result = await Fylo.getDoc<_user>(USERS, sampleId).once()
        const user = result[sampleId]

        expect(user).toBeDefined()
        expect(typeof user.name).toBe('string')
        expect(typeof user.email).toBe('string')
        expect(typeof user.phone).toBe('string')
    })

    test("GET ONE — first-level nested object is reconstructed correctly", async () => {

        const result = await Fylo.getDoc<_user>(USERS, sampleId).once()
        const user = result[sampleId]

        expect(user.address).toBeDefined()
        expect(typeof user.address.city).toBe('string')
        expect(typeof user.address.street).toBe('string')
        expect(typeof user.address.zipcode).toBe('string')
    })

    test("GET ONE — deeply nested object is reconstructed correctly", async () => {

        const result = await Fylo.getDoc<_user>(USERS, sampleId).once()
        const user = result[sampleId]

        expect(user.address.geo).toBeDefined()
        expect(typeof user.address.geo.lat).toBe('number')
        expect(typeof user.address.geo.lng).toBe('number')
    })

    test("GET ONE — second nested object is reconstructed correctly", async () => {

        const result = await Fylo.getDoc<_user>(USERS, sampleId).once()
        const user = result[sampleId]

        expect(user.company).toBeDefined()
        expect(typeof user.company.name).toBe('string')
        expect(typeof user.company.catchPhrase).toBe('string')
        expect(typeof user.company.bs).toBe('string')
    })

    test("SELECT — nested values are not corrupted across documents", async () => {

        for await (const data of Fylo.findDocs<_user>(USERS).collect()) {

            const [, user] = Object.entries(data as Record<_ttid, _user>)[0]

            expect(user.address).toBeDefined()
            expect(user.address.geo).toBeDefined()
            expect(typeof user.address.geo.lat).toBe('number')
            expect(user.company).toBeDefined()
        }
    })

    test("$select — returns only requested top-level fields", async () => {

        let results: Record<_ttid, _user> = {}

        for await (const data of Fylo.findDocs<_user>(USERS, { $select: ['name', 'email'] }).collect()) {
            results = { ...results, ...data as Record<_ttid, _user> }
        }

        const users = Object.values(results)
        const onlyNameAndEmail = users.every(u => u.name && u.email && !u.phone && !u.address)

        expect(onlyNameAndEmail).toBe(true)
    })

    test("$eq on nested string field — query by city", async () => {

        const result = await Fylo.getDoc<_user>(USERS, sampleId).once()
        const targetCity = result[sampleId].address.city

        let results: Record<_ttid, _user> = {}

        for await (const data of Fylo.findDocs<_user>(USERS, {
            $ops: [{ ['address/city' as keyof _user]: { $eq: targetCity } }]
        }).collect()) {
            results = { ...results, ...data as Record<_ttid, _user> }
        }

        const matchingUsers = Object.values(results)
        const allMatch = matchingUsers.every(u => u.address.city === targetCity)

        expect(allMatch).toBe(true)
        expect(matchingUsers.length).toBeGreaterThan(0)
    })
})

describe("SQL — dot notation", async () => {

    test("WHERE with dot notation — first-level nested field", async () => {

        const result = await Fylo.getDoc<_user>(USERS, sampleId).once()
        const targetCity = result[sampleId].address.city

        const results = await fylo.executeSQL<_user>(
            `SELECT * FROM ${USERS} WHERE address.city = '${targetCity}'`
        ) as Record<_ttid, _user>

        const users = Object.values(results)
        const allMatch = users.every(u => u.address.city === targetCity)

        expect(allMatch).toBe(true)
        expect(users.length).toBeGreaterThan(0)
    })

    test("WHERE with dot notation — deeply nested field", async () => {

        const result = await Fylo.getDoc<_user>(USERS, sampleId).once()
        const targetLat = result[sampleId].address.geo.lat

        const results = await fylo.executeSQL<_user>(
            `SELECT * FROM ${USERS} WHERE address.geo.lat = '${targetLat}'`
        ) as Record<_ttid, _user>

        const users = Object.values(results)
        const allMatch = users.every(u => u.address.geo.lat === targetLat)

        expect(allMatch).toBe(true)
        expect(users.length).toBeGreaterThan(0)
    })

    test("WHERE with dot notation — second nested object", async () => {

        const result = await Fylo.getDoc<_user>(USERS, sampleId).once()
        const targetCompany = result[sampleId].company.name

        const results = await fylo.executeSQL<_user>(
            `SELECT * FROM ${USERS} WHERE company.name = '${targetCompany}'`
        ) as Record<_ttid, _user>

        const users = Object.values(results)
        const allMatch = users.every(u => u.company.name === targetCompany)

        expect(allMatch).toBe(true)
        expect(users.length).toBeGreaterThan(0)
    })

    test("SELECT with dot notation in WHERE — partial field selection", async () => {

        const result = await Fylo.getDoc<_user>(USERS, sampleId).once()
        const targetCity = result[sampleId].address.city

        const results = await fylo.executeSQL<_user>(
            `SELECT name, email FROM ${USERS} WHERE address.city = '${targetCity}'`
        ) as Record<_ttid, _user>

        const users = Object.values(results)

        expect(users.length).toBeGreaterThan(0)
        expect(users.every(u => u.name && u.email && !u.phone)).toBe(true)
    })
})
