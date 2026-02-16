from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient()
db = client.tracking
vehicle_collection = db['vehicle_inventory']

async def get_vehicle_profile(imei: str) -> str:
    vehicle = await db.vehicles.find_one({"imei": imei})
    if vehicle and vehicle.get("vehicle_profile"):
        return vehicle["vehicle_profile"]
    return "generic_unknown"
