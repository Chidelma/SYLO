export default class ULID {

    static isULID(_id: string) {

        return _id.match(/^[A-Z0-9]+-(?:[2-9]|[1-2][0-9]|3[0-6])-[A-Z0-9]+$/i)
    }

    static isUUID(_id: string) {

        return _id.match(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i)
    }

    static generate() {

        const time = Date.now()

        const nums = String(time).split('').map(Number)

        const base = ULID.calcLuhnBase(nums)

        const prefix = time.toString(base)

        const arr = new Uint8Array(nums.length)

        crypto.getRandomValues(arr)

        const suffix = Number(arr.join('').slice(0, nums.length)).toString(base)

        return `${prefix}-${base}-${suffix}`.toUpperCase() as _ulid
    }

    private static calcLuhnBase(nums: number[]) {

        let sum = 0

        for (let i = nums.length - 2; i >= 0; i -= 2) {
            
            const doubled = nums[i] * 2

            if (doubled > 9) sum += doubled % 10 + 1
            else sum += doubled
        }

        return sum > 36 ? 36 : sum
    }

    static decodeTime(_id: string) {

        if (!ULID.isULID(_id)) throw new Error('Invalid ULID')

        const segs = _id.split('-')

        return parseInt(segs[0], Number(segs[1]))
    }
}                                      