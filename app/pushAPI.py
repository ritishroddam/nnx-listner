from datetime import datetime
from pymongo import MongoClient

mongo_client = MongoClient("mongodb+srv://doadmin:4T81NSqj572g3o9f@db-mongodb-blr1-27716-c2bd0cae.mongo.ondigitalocean.com/admin?tls=true&authSource=admin", tz_aware=True)
db = mongo_client["nnx"]

moveInSyncSubscribed = db ['moveInSyncSubscriptions']

def sendDataToMoveInSync(data, alerts, date_time):
    print(date_time.timestamp())

async def sendPushAPIs(data, alerts, date_time):
    try:
        sendDataToMoveInSync(data, alerts, date_time)
    
    except Exception as e:
        print(f"[ERROR] in dataToAlertParser: {e}")