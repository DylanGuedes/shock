from pymongo import MongoClient
import json
from bson import json_util

db_name = 'sp'
collections_name = ['olho_vivo']

client = MongoClient()
db = client[db_name]

for coll in collections_name:
    collection = db[coll].find({"prefix": 81132}).limit(100000)
    print("*Inserting {0} in /tmp/{1}_{2}.json*".format(coll, db_name, coll))
    from_mongo = json_util.dumps(collection)
    from_json = json.loads(from_mongo)
    json.dump(from_json, open("/tmp/{0}_{1}.json".format(db_name, coll), "w"))
