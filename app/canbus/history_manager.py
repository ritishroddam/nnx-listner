from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime

client = AsyncIOMotorClient()
db = client.tracking

THRESHOLD = {"engine_rpm":50,"speed_kmh":1,"fuel_rate_lph":0.2,"soc_pct":1}

async def store_can_history_if_changed(imei, new_signals):
    last = await db.vehicle_can_state.find_one({"imei": imei})
    old = last["signals"] if last else {}
    changed = {}

    for k,v in new_signals.items():
        if abs(v - old.get(k, 0)) > THRESHOLD.get(k,0):
            changed[k] = v

    if changed:
        await db.vehicle_can_history.insert_one({
            "imei": imei,
            "timestamp": datetime.utcnow(),
            "signals": changed
        })
