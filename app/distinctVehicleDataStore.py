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
atlantaAis140 = db['atlantaAis140']
atlantaAis140Status = db['atlantaAis140Status']

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
        {"$match": {"date_time": {"$gte": seven_days_ago, "$lte": utc_now}}},
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
        status_collection.update_one(
            {"_id": doc["_id"]},
            {"$set": doc},
            upsert=True
        )
        
    pipeline = [
        {"$match": {"timestamp": {"$gte": seven_days_ago, "$lte": utc_now}}},
        {"$sort": {"timestamp": -1}},
        {"$group": {
            "_id": "$imei",
            "timestamp": {"$first": "$timestamp"},
            "ignition": {"$first": "$telemetry.ignition"},
            "speed": {"$first": "$telemetry.speed"},
            "date": {"$first": "$gps.date"},
            "time": {"$first": "$gps.time"},
            "gsm_sig": {"$first": "$network.gsmSignal"},
            "history": {"$push": {
                "date_time": "$timestamp",
                "ignition": "$telemetry.ignition",
                "speed": "$telemetry.speed"
            }}
        }},
        {"$project": {
            "_id": 1,
            "latest": {
                "date_time": "$timestamp",
                "ignition": "$ignition",
                "speed": "$speed",
                "date": "$date",
                "time": "$time",
                "gsm_sig": "$gsm_sig"
            },
            "history": 1
        }},
    ]

    atlantaais140Results = list(atlantaAis140.aggregate(pipeline))

    for doc in atlantaais140Results:        
        atlantaAis140Status.update_one(
            {"_id": doc["_id"]},
            {"$set": doc},
            upsert=True
        )

if __name__ == '__main__':
    while True:
        update_distinct_atlanta()
        atlantaStatusData()
        time.sleep(10)  # Wait for 60 seconds before running the function again