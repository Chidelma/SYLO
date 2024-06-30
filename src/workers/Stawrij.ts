import Silo from '../src/Stawrij'

self.onmessage = async (ev) => {

    const { action, data } = ev.data

    switch(action) {

        case 'PUT':
            self.postMessage(await Silo.putDoc(data.collection, data.doc))
            break
        case 'PATCH':
            self.postMessage(await Silo.patchDoc(data.collection, data.doc))
            break
        case 'DEL':
            self.postMessage(await Silo.delDoc(data.collection, data.id))
            break
        default:
            console.log(`Action ${action} not implemented`)
            self.postMessage({})
            break
    }
}