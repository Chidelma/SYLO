import { _schema } from '../types/schema'
import Silo from '../Stawrij'

export default class {

    static async putData(container: string, key: string, value: string) {

        const containerClient = Silo.blob!.getContainerClient(container)

        await containerClient.getBlockBlobClient(key).uploadData(Buffer.from(value))
    }

    static async delData(container: string, key: string) {

        const containerClient = Silo.blob!.getContainerClient(container)

        await containerClient.getBlockBlobClient(key).delete()
    }
}