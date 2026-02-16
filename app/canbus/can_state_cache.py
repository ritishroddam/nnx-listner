from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime

client = AsyncIOMotorClient()
db = client.tracking

async def update_can_state(imei, signals):
    if not signals:
        return
    await db.vehicle_can_state.update_one(
        {"imei": imei},
        {"$set": {"signals": signals, "updated_at": datetime.utcnow()}},
        upsert=True
    )

async def get_last_can_state(imei):
    doc = await db.vehicle_can_state.find_one({"imei": imei})
    return doc["signals"] if doc else {}
