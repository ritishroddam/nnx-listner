from pymongo import MongoClient
from datetime import datetime, timedelta
from pytz import timezone
import time
import socketio
from pymongo import MongoClient
import socketio

sio = socketio.Client(ssl_verify=False)  # Disable verification for self-signed certs

mongo_client = MongoClient("mongodb+srv://doadmin:4T81NSqj572g3o9f@db-mongodb-blr1-27716-c2bd0cae.mongo.ondigitalocean.com/admin?tls=true&authSource=admin")
db = mongo_client["nnx"]
atlanta_collection = db['atlanta']
distinct_atlanta_collection = db['distinctAtlanta']
vehicle_inventory_collection = db['vehicle_inventory']
status_collection = db['statusAtlanta']

def update_distinct_atlanta():
    try:
        print("Successfully running distinct Vehicle")
        all_documents = list(atlanta_collection.aggregate([
                {"$match": {"gps": "A"}},
                {"$sort": {"date_time": -1}},
                {
                    "$group": {
                        "_id": "$imei",
                        "latest_doc": {"$first": "$$ROOT"}
                    }
                },
                {"$replaceRoot": {"newRoot": "$latest_doc"}}
            ]))
        
        print(f"Fetched {len(all_documents)} documents from the atlanta collection")

        for doc in all_documents:
            doc.pop('_id', None)

        distinct_atlanta_collection.delete_many({})

        if all_documents:
            distinct_atlanta_collection.insert_many(all_documents)

        print('Distinct documents updated successfully')

    except Exception as e:
        print(f'Error updating distinct documents: {str(e)}')
        
def atlantaStatusData():
    utc_now = datetime.now(timezone('UTC'))
    seven_days_ago = utc_now - timedelta(days=7)
    pipeline = [
        {"$match": {"date_time": {"$gte": seven_days_ago}}},
        {"$sort": {"date_time": -1}},
        {"$group": {
            "_id": "$imei",
            "latest": {"$first": "$$ROOT"},
            "history": {"$push": {
                "date_time": "$date_time",
                "ignition": "$ignition",
                "speed": "$speed"
            }}
        }},
    ]
    results = list(atlanta_collection.aggregate(pipeline))
    for doc in results:
        if doc['latest']['date_time'] < datetime.now(timezone('UTC')):
            for doc_history in doc['history']:
                if doc_history['date_time'] < datetime.now(timezone('UTC')):
                    doc['history'].remove(doc_history)
                    
            status_collection.update_one(
                {"_id": doc["_id"]},
                {"$set": doc},
                upsert=True
            )

if __name__ == '__main__':
    while True:
        update_distinct_atlanta()
        atlantaStatusData()
        time.sleep(10)  # Wait for 60 seconds before running the function again