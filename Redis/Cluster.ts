import Silo from '../Stawrij'

export default class {

    static async putData(key: string, value: string) {

        await Silo.redis.set(key, value)
    }

    static async delData(key: string) {

        await Silo.redis.del(key)
    }
}