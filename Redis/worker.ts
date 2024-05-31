import red from './Cluster'

self.onmessage = async (ev) => {

    const { action, data } = ev.data

    switch(action) {

        case 'PUT':
            self.postMessage(await red.putData(data.key, data.val))
            break
        case 'DEL':
            self.postMessage(await red.delData(data.key))
            break
        default:
            console.warn(`Action ${action} not implemented`)
            self.postMessage({})
            break
    }
}