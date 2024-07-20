import Silo from '../Stawrij'

self.onmessage = async (ev) => {

    const { action, data } = ev.data

    switch(action) {

        case 'PUT':
            self.postMessage(await Silo.putData(data.collection, data.doc))
            break
        case 'PATCH':
            self.postMessage(await Silo.patchDoc(data.collection, data.doc))
            break
        case 'DEL':
            self.postMessage(await Silo.delDoc(data.collection, data._id))
            break
        default:
            console.log(`Action ${action} not implemented`)
            self.postMessage({})
            break
    }
}