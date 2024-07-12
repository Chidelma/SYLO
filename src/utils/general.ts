export function invokeWorker(url: string, message: any, resolve: () => void, result?: any) {

    const worker = new Worker(url)

    worker.onmessage = ev => {
        if(result) {
            if(Array.isArray(result)) result.push(ev.data)
            else result = ev.data
        } 
        worker.terminate()
        resolve()
    }

    worker.onerror = ev => {
        console.error(ev.message)
        worker.terminate()
        resolve()
    }
    
    worker.postMessage(message)
}