import Dir from '../Directory'

self.onmessage = async (ev) => {

    const { action, data } = ev.data

    switch(action) {

        case 'GET':
            self.postMessage(await Dir.reconstructData(data.collection, data.id))
            break
        case 'PUT':
            self.postMessage(await Dir.updateIndex(data.idx))
            break
        case 'DEL':
            self.postMessage(Dir.deleteIndex(data.idx))
            break
        default:
            console.log(`Action ${action} not implemented`)
            self.postMessage({})
            break
    }
}