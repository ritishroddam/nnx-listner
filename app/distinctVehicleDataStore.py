from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
import time
import socketio
from pymongo import MongoClient
import socketio

sio = socketio.Client(ssl_verify=False)  # Disable verification for self-signed certs

mongo_client = MongoClient("mongodb://doadmin:4l239y815dQan0Vo@mongodb+srv://cordonnxDB-4f7df3c7.mongo.ondigitalocean.com/?tls=true&authSource=admin")
db = mongo_client["nnx"]
atlanta_collection = db['atlanta']
distinct_atlanta_collection = db['distinctAtlanta']
vehicle_inventory_collection = db['vehicle_inventory']
status_collection = db['statusAtlanta']
atlantaAis140 = db['atlantaAis140']
atlantaAis140Status = db['atlantaAis140Status']

def _convert_date_time_emit(date):
    if not date:
        now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        return now.strftime("%d%m%y"), now.strftime("%H%M%S")
    # Convert UTC datetime to IST (UTC+5:30)
    ist = date.astimezone(timezone(timedelta(hours=5, minutes=30)))
    return ist.strftime("%d%m%y"), ist.strftime("%H%M%S")

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
    utc_now = datetime.now(timezone.utc)
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
        {"$match": {"gps.timestamp": {"$gte": seven_days_ago, "$lte": utc_now}}},
        {"$sort": {"gps.timestamp": -1}},
        {"$group": {
            "_id": "$imei",
            "timestamp": {"$first": "$gps.timestamp"},
            "ignition": {"$first": "$telemetry.ignition"},
            "speed": {"$first": "$telemetry.speed"},
            "gsm_sig": {"$first": "$network.gsmSignal"},
            "history": {"$push": {
                "date_time": "$gps.timestamp",
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
                "gsm_sig": "$gsm_sig"
            },
            "history": 1
        }},
    ]

    atlantaais140Results = list(atlantaAis140.aggregate(pipeline))

    for doc in atlantaais140Results:
        doc["latest"]["date"], doc["latest"]["time"] = _convert_date_time_emit(doc["latest"]["date_time"])
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