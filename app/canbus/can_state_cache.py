from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timezone

MONGO_URI = "mongodb+srv://doadmin:U6bOV204y9r75Iz3@private-db-mongodb-blr1-96186-4485159f.mongo.ondigitalocean.com/admin?tls=true&authSource=admin&replicaSet=db-mongodb-blr1-96186"
mongo_client: AsyncIOMotorClient = AsyncIOMotorClient(MONGO_URI, tz_aware=True)

db = mongo_client['nnx']
vehicle_can_state_collection = db['vehicle_can_state']

async def update_can_state(imei, signals, ts):
    print(f"[DEBUG] Updating CAN state for IMEI: {imei} with signals: {signals}")
    if not signals:
        return
    device_can_data = await vehicle_can_state_collection.find_one({"imei": imei}, {"updated_at": 1})
    last_updated_timestamp = device_can_data["updated_at"] if device_can_data else None
    if (ts - last_updated_timestamp).total_seconds() > 0:
        await vehicle_can_state_collection.update_one(
            {"imei": imei},
            {"$set": {"signals": signals, "updated_at": ts}},
            upsert=True
        )

async def get_last_can_state(imei):
    doc = await vehicle_can_state_collection.find_one({"imei": imei})
    return doc["signals"] if doc else {}
