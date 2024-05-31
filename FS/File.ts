import { _schema } from '../types/schema'
import { existsSync, unlinkSync } from "fs"

export default class {

    private static readonly DATA_PATH = process.env.DATA_PREFIX ?? `${process.cwd()}/db`

    static async putData(key: string, val: any) {
        await Bun.write(`${this.DATA_PATH}/${key}`, val)
    }

    static delData(key: string) {
        if(existsSync(`${this.DATA_PATH}/${key}`)) unlinkSync(`${this.DATA_PATH}/${key}`)
        else console.warn(`${this.DATA_PATH}/${key} does not exist`)
    }
}