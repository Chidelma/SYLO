import FS from './File'
import Silo from '../Stawrij'

self.onmessage = async (ev) => {

    const { action, data } = ev.data

    switch(action) {

        case 'GET':
            self.postMessage(await Silo.getDoc(data.collection, data.id))
            break
        case 'PUT':
            self.postMessage(await FS.putData(data.key, data.val))
            break
        case 'DEL':
            self.postMessage(FS.delData(data.key))
            break
        default:
            console.warn(`Action ${action} not implemented`)
            self.postMessage({})
            break
    }
}