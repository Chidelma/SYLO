import { _schema } from '../types/schema'
import Silo from '../Stawrij'

export default class {

    static async putData(bucket: string, key: string, value: string) {

        await Silo.stawr!.bucket(bucket).file(key).save(value)
    }

    static async delData(bucket: string, key: string) {

        await Silo.stawr!.bucket(bucket).file(key).delete()
    }
}