//@ts-ignore
declare var self: Worker

self.onmessage = async (event: MessageEvent) => {

    try {

        const func: Promise<any> = event.data.func

        self.postMessage({ success: await func })
    
    } catch(e) {

        self.postMessage({ error: e.message })
    }
}