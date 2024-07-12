import Walker from '../Walker'

self.onmessage = async (ev) => {

    const { action, data } = ev.data

    switch(action) {

        case 'GET':
            self.postMessage(Walker.search(data.pattern))
            break
        default:
            console.log(`Action ${action} not implemented`)
            self.postMessage({})
            break
    }
}