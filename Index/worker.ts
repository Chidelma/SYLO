import Dir from './Directory'

self.onmessage = async (ev) => {

    const { action, idx } = ev.data

    switch(action) {

        case 'PUT':
            self.postMessage(await Dir.updateIndex(idx))
            break
        case 'DEL':
            self.postMessage(Dir.deleteIndex(idx))
            break
        default:
            console.log(`Action ${action} not implemented`)
            self.postMessage({})
            break
    }
}