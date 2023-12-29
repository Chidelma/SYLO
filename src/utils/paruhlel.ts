interface _worker {
    success?: any,
    error?: any
}

export function executeInParallel<T>(promises: Promise<T>[]) {

    return new Promise((resolve: (values: T[]) => void) => {

        const workers: Worker[] = []
        const results: T[] = []

        const promiseLen = promises.length
        let count = 0;

        const handleWorkerMessage = (result: _worker) => {

            count++

            if(result.success) results.push(result.success)
            if(result.error) console.error(result.error)

            if(count === promiseLen) {
                workers.forEach((worker) => worker.terminate())
                resolve(results)
            }
        } 

        promises.forEach((promise) => {

            const worker = new Worker(new URL('worker.ts', import.meta.url).href)

            worker.onmessage = (event: MessageEvent) => handleWorkerMessage(event.data)

            worker.postMessage({ func: promise })
        })
    })
}