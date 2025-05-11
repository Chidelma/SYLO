import ULID from "../src/ULID";
import { test, expect, describe } from 'bun:test'

describe("ULID", () => {

    test("Generate", () => {

        const _id = ULID.generate()

        const [created, base, updated] = _id.split('-')

        const { createdAt, updatedAt } = ULID.decodeTime(_id)

        expect(Number(base)).toBeGreaterThanOrEqual(18)
        expect(Number(base)).toBeLessThanOrEqual(36)
        expect(created).toEqual(updated)
        expect(ULID.isULID(_id)).not.toBeNull()
        expect(ULID.isUUID(_id)).toBeNull()
        expect(ULID.isUUID(Bun.randomUUIDv7())).not.toBeNull()
        expect(typeof createdAt).toBe('number')
        expect(typeof updatedAt).toBe('number')
        expect(createdAt).toEqual(updatedAt)
    })

    test("Update", async () => {

        const _id = ULID.generate()
        await Bun.sleep(1000)
        const _newId = ULID.update(_id)

        const [created, base, updated] = _newId.split('-')

        const { createdAt, updatedAt } = ULID.decodeTime(_newId)

        expect(Number(base)).toBeGreaterThanOrEqual(18)
        expect(Number(base)).toBeLessThanOrEqual(36)
        expect(created).not.toEqual(updated)
        expect(ULID.isULID(_newId)).not.toBeNull()
        expect(ULID.isUUID(_newId)).toBeNull()
        expect(_id).not.toEqual(_newId)
        expect(typeof createdAt).toBe('number')
        expect(typeof updatedAt).toBe('number')
        expect(createdAt).not.toEqual(updatedAt)
        expect(updatedAt).toBeGreaterThan(createdAt)
    })
})