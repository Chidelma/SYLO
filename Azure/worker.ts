import Az from './Blob'

self.onmessage = async (ev) => {

    const { action, data } = ev.data

    switch(action) {

        case 'PUT':
            self.postMessage(await Az.putData(data.silo, data.key, data.val))
            break
        case 'DEL':
            self.postMessage(await Az.delData(data.silo, data.key))
            break
        default:
            console.warn(`Action ${action} not implemented`)
            self.postMessage({})
            break
    }
}