import { PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3'
import { _schema } from '../types/schema'
import Silo from '../Stawrij'


export default class {

    static async putData(bucket: string, key: string, value: any) {
        await Silo.s3!.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: value}))
    }

    static async delData(bucket: string, key: string) {
        await Silo.s3!.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }))
    }
}