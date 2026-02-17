from motor.motor_asyncio import AsyncIOMotorClient

MONGO_URI = "mongodb+srv://doadmin:U6bOV204y9r75Iz3@private-db-mongodb-blr1-96186-4485159f.mongo.ondigitalocean.com/admin?tls=true&authSource=admin&replicaSet=db-mongodb-blr1-96186"
mongo_client: AsyncIOMotorClient = AsyncIOMotorClient(MONGO_URI, tz_aware=True)

db = mongo_client['nnx']
vehicle_collection = db['vehicle_inventory']

async def get_vehicle_profile(imei: str) -> str:
    vehicle = await vehicle_collection.find_one({"IMEI": imei})
    print(f"[DEBUG] Vehicle data for IMEI {imei}:", vehicle)
    if vehicle and vehicle.get("vehicle_profile"):
        return vehicle["vehicle_profile"]
    return "generic_unknown"
