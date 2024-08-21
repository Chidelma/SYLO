import Silo from '../src/Stawrij'

await Silo.importBulkData<_tips>('tips', new URL('file:///Volumes/APFS-SSD/yelp-db/tips.json'))