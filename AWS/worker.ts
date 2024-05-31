import S3 from './S3'

self.onmessage = async (ev) => {

    const { action, data } = ev.data

    switch(action) {

        case 'PUT':
            self.postMessage(await S3.putData(data.silo, data.key, data.value))
            break
        case 'DEL':
            self.postMessage(await S3.delData(data.silo, data.key))
            break
        default:
            console.warn(`Action ${action} not implemented`)
            self.postMessage({})
            break
    }
}