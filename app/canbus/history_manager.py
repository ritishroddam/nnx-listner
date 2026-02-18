from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timezone

MONGO_URI = "mongodb+srv://doadmin:U6bOV204y9r75Iz3@private-db-mongodb-blr1-96186-4485159f.mongo.ondigitalocean.com/admin?tls=true&authSource=admin&replicaSet=db-mongodb-blr1-96186"
mongo_client: AsyncIOMotorClient = AsyncIOMotorClient(MONGO_URI, tz_aware=True)

db = mongo_client['nnx']
vehicle_can_state_collection = db['vehicle_can_state']
vehicle_can_history_collection = db['vehicle_can_history']

THRESHOLD = {"engine_rpm":50,"speed_kmh":1,"fuel_rate_lph":0.2,"soc_pct":1}

async def store_can_history_if_changed(imei, new_signals, ts):
    print(f"[DEBUG] Storing CAN history for IMEI: {imei} with signals: {new_signals}")
    try:
        last = await vehicle_can_state_collection.find_one({"imei": imei})
        old = last["signals"] if last else {}
        changed = {}

        for k,v in new_signals.items():
            try:
                v_float = float(v) if isinstance(v, str) else v
                old_val = float(old.get(k, 0)) if isinstance(old.get(k, 0), str) else old.get(k, 0)
                if abs(v_float - old_val) > THRESHOLD.get(k, 0):
                    changed[k] = v
            except (ValueError, TypeError):
                if v != old.get(k):
                    changed[k] = v

        if changed:
            await vehicle_can_history_collection.insert_one({
                "imei": imei,
                "timestamp": ts,
                "signals": changed
            })
    except Exception as e:
        print(f"[ERROR] in store_can_history_if_changed for IMEI {imei}: {e}")