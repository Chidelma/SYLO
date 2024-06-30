import Silo from '../src/Stawrij'

const file = Bun.file('/Volumes/EFS/yelp_dataset/yelp_academic_dataset_checkin.json')

const res = new Response(file)

const reader = res.body!.getReader()

const decoder = new TextDecoder()

let lastLineIncomplete = false
let incompleteLine = ''

while(true) {

    const data = await reader.read()

    if(data.done) break

    const decoded = decoder.decode(data.value)

    const lines = decoded.split('\n')

    for(let line of lines) {

        try {

            if(lastLineIncomplete) {
                line = incompleteLine + line
                lastLineIncomplete = false
                incompleteLine = ''
            }

            await Silo.putDoc('checkins', JSON.parse(line))

        } catch(e) {
            lastLineIncomplete = true
            incompleteLine = line
            break
        }
    }
}