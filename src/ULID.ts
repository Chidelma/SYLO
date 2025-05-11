export default class ULID {

    private static multiple = 10000

    private static minBase = 18

    static isULID(_id: string) {

        return _id.match(/^[A-Z0-9]+-(?:[2-9]|[1-2][0-9]|3[0-6])-[A-Z0-9]+$/i)
    }

    static isUUID(_id: string) {

        return _id.match(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i)
    }

    static generate() {

        const time = (performance.now() + performance.timeOrigin) * ULID.multiple
        
        const nums = String(time).split('').map(Number)

        const base = ULID.getRandomBase(nums)

        const timeCode = time.toString(base)

        return `${timeCode}-${base}-${timeCode}`.toUpperCase() as _ulid
    }

    private static getRandomBase(nums: number[]) {

        return nums.reverse().join('').split('').map(Number).reduce((prev, curr) => {
            
            if(prev < ULID.minBase) prev += curr

            return prev

        }, 0)
    }

    static update(_id: string) {

        if (!ULID.isULID(_id)) throw new Error('Invalid ULID')

        const [created, stringBase] = _id.split('-')

        const time = (performance.now() + performance.timeOrigin) * ULID.multiple

        const timeCode = time.toString(Number(stringBase))

        return `${created}-${stringBase}-${timeCode}`.toUpperCase() as _ulid
    }

    static decodeTime(_id: string) {

        if (!ULID.isULID(_id)) throw new Error('Invalid ULID')

        const [created, stringBase, updated] = _id.split('-')

        const base = Number(stringBase)

        const convertToMilliseconds = (timeCode: string) => Number((parseInt(timeCode, base) / ULID.multiple).toFixed(0))

        return {
            createdAt: convertToMilliseconds(created),
            updatedAt: convertToMilliseconds(updated)
        }
    }
}                                      