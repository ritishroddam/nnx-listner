from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
import time
import socketio
from pymongo import MongoClient
import socketio

sio = socketio.Client(ssl_verify=False)  # Disable verification for self-signed certs

mongo_client = MongoClient("mongodb+srv://doadmin:U6bOV204y9r75Iz3@private-db-mongodb-blr1-96186-4485159f.mongo.ondigitalocean.com/admin?tls=true&authSource=admin&replicaSet=db-mongodb-blr1-96186")
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
        imeis = atlanta_collection.distinct('imei')
        
        for imei in imeis:
            latest_doc = atlanta_collection.find_one(
                {"imei": imei}, 
                sort=[("date_time", -1)]
            )
            
            if latest_doc:
                latest_doc.pop('_id', None)
                distinct_atlanta_collection.update_one(
                    {"_id": imei},
                    {"$set": latest_doc},
                    upsert=True
                )

    except Exception as e:
        print(f'Error updating distinct documents: {str(e)}')
        
def atlantaStatusData():
    utc_now = datetime.now(timezone.utc)
    seven_days_ago = utc_now - timedelta(days=7)

    cursor = atlanta_collection.find(
        {"date_time": {"$gte": seven_days_ago, "$lte": utc_now}},
        {
            "_id": 0,
            "imei": 1,
            "date_time": 1,
            "ignition": 1,
            "speed": 1,
        },
    ).sort("date_time", -1)

    grouped = {}
    for doc in cursor:
        imei = doc.get("imei")
        if not imei:
            continue
        if imei not in grouped:
            grouped[imei] = {
                "_id": imei,
                "latest": doc,
                "history": [],
            }
        grouped[imei]["history"].append(
            {
                "date_time": doc.get("date_time"),
                "ignition": doc.get("ignition"),
                "speed": doc.get("speed"),
            }
        )

    for doc in grouped.values():
        status_collection.update_one({"_id": doc["_id"]}, {"$set": doc}, upsert=True)

    cursor = atlantaAis140.find(
        {"gps.timestamp": {"$gte": seven_days_ago, "$lte": utc_now}},
        {
            "_id": 0,
            "imei": 1,
            "gps.timestamp": 1,
            "telemetry.ignition": 1,
            "telemetry.speed": 1,
            "network.gsmSignal": 1,
        },
    ).sort("gps.timestamp", -1)

    grouped_ais = {}
    for doc in cursor:
        imei = doc.get("imei")
        if not imei:
            continue
        ts = doc.get("gps", {}).get("timestamp")
        ignition = doc.get("telemetry", {}).get("ignition")
        speed = doc.get("telemetry", {}).get("speed")
        gsm_sig = doc.get("network", {}).get("gsmSignal")

        if imei not in grouped_ais:
            grouped_ais[imei] = {
                "_id": imei,
                "latest": {
                    "date_time": ts,
                    "ignition": ignition,
                    "speed": speed,
                    "gsm_sig": gsm_sig,
                },
                "history": [],
            }

        grouped_ais[imei]["history"].append(
            {"date_time": ts, "ignition": ignition, "speed": speed}
        )

    for doc in grouped_ais.values():
        doc["latest"]["date"], doc["latest"]["time"] = _convert_date_time_emit(
            doc["latest"]["date_time"]
        )
        atlantaAis140Status.update_one({"_id": doc["_id"]}, {"$set": doc}, upsert=True)

if __name__ == '__main__':
    while True:
        update_distinct_atlanta()
        atlantaStatusData()
        time.sleep(10)  # Wait for 60 seconds before running the function again