from datetime import datetime, timedelta, timezone
import time
from pymongo import ASCENDING, DESCENDING, MongoClient
import eventlet

from parser import atlantaAis140ToFront
from map_server import geocodeInternal

mongo_client = MongoClient("mongodb+srv://doadmin:U6bOV204y9r75Iz3@private-db-mongodb-blr1-96186-4485159f.mongo.ondigitalocean.com/admin?tls=true&authSource=admin&replicaSet=db-mongodb-blr1-96186", tz_aware=True)
db = mongo_client["nnx"]

live_map_collection = db['live_map']
atlanta_collection = db['atlanta']
atlantaLatest_collection = db['atlantaLatest']
vehicle_inventory_collection = db['vehicle_inventory']
company_collection = db['customers_list']
status_collection = db['statusAtlanta']
atlantaAis140_collection = db['atlantaAis140']
atlantaAis140Latest_collection = db['atlantaAis140_latest']
atlantaAis140Status_collection = db['atlantaAis140Status']

def format_seconds(seconds):
    seconds = int(seconds/1000)
    if seconds >= 86400:
        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        return f"{days} days"
    elif seconds >= 3600:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours} hours"
    elif seconds >= 60:
        minutes = seconds // 60
        sec = seconds % 60
        return f"{minutes} minutes"
    else:
        return f"{seconds} seconds"

def getDistanceBasedOnTime(imeis, fromDate, toDate):
    pool = eventlet.GreenPool(size=10)
    distances = []

    def worker(imei):
        start = atlanta_collection.find_one(
            {"imei": imei, "date_time": {"$gte": fromDate, "$lt": toDate}},
            sort=[("date_time", ASCENDING)],
        )
        if start:
            end = atlanta_collection.find_one(
                {"imei": imei, "date_time": {"$gte": fromDate, "$lt": toDate}},
                sort=[("date_time", DESCENDING)],
            )
            return {
                "imei": imei,
                "first_odometer": start.get("odometer", 0),
                "last_odometer": end.get("odometer", 0),
            }

        start = atlantaAis140_collection.find_one(
            {"imei": imei, "gps.timestamp": {"$gte": fromDate, "$lt": toDate}},
            sort=[("gps.timestamp", ASCENDING)],
        )
        if not start:
            return None
        end = atlantaAis140_collection.find_one(
            {"imei": imei, "gps.timestamp": {"$gte": fromDate, "$lt": toDate}},
            sort=[("gps.timestamp", DESCENDING)],
        )
        return {
            "imei": imei,
            "first_odometer": start.get("telemetry", {}).get("odometer", 0),
            "last_odometer": end.get("telemetry", {}).get("odometer", 0),
        }

    for result in pool.imap(worker, imeis):
        if result:
            distances.append(result)

    return distances
    
def getVehicleStatus(imei_list):
    try:
        utc_now = datetime.now(timezone.utc)
        twenty_four_hours_ago = utc_now - timedelta(hours=24)
        
        results = list(status_collection.find({"_id": {"$in": imei_list}}))
        
        for imei in imei_list:
            data = atlantaAis140Status_collection.find_one({"_id": imei})
            if data:
                results.append(data)
        
        statuses = []

        for item in results:
            imei = item["_id"]
            latest = item["latest"]
            history = item["history"]
            now = utc_now

            if latest["date_time"] < twenty_four_hours_ago:
                status = "offline"
                status_time_delta = (now - latest["date_time"]).total_seconds() * 1000
                status_time_str = format_seconds(status_time_delta)
            else:
                ignition = str(latest.get("ignition"))
                speed = float(latest.get("speed", 0))
                current_status = None
                if ignition == "0":
                    current_status = "stopped"
                elif ignition == "1" and speed > 0:
                    current_status = "moving"
                elif ignition == "1" and speed == 0.0:
                    current_status = "idle"
                else:
                    current_status = "unknown"

                last_change_time = latest["date_time"]
                for h in history[1:]:
                    h_ignition = str(h.get("ignition"))
                    h_speed = float(h.get("speed", 0))
                    if current_status == "moving" and not (h_ignition == "1" and h_speed > 0):
                        break
                    if current_status == "idle" and not (h_ignition == "1" and h_speed == 0):
                        break
                    if current_status == "stopped" and not (h_ignition == "0"):
                        break
                    last_change_time = h["date_time"]

                status = current_status
                status_time_delta = (now - last_change_time).total_seconds() * 1000
                status_time_str = format_seconds(status_time_delta)

            statuses.append({
                "imei": imei,
                "status": status,
                "status_time_delta": status_time_delta,
                "status_time_str": status_time_str,
                "date": latest["date"],
                "time": latest["time"],
                "ignition": latest.get("ignition"),
                "speed": latest.get("speed"),
                "gsm_sig": latest.get("gsm_sig"),
            })
        missingImeis = set(imei_list) - {item['imei'] for item in statuses}
        return (statuses, missingImeis)

    except Exception as e:
        print(f"Error in getVehicleStatus: {e}")
        return []

def getStopTimeToday(imeis):
    try:
        now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        ist_start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        ist_end_of_day = ist_start_of_day + timedelta(days=1)
        
        start_of_day = ist_start_of_day.astimezone(timezone.utc)
        end_of_day = ist_end_of_day.astimezone(timezone.utc)
        
        resultItems = []
        for imei in imeis:
            currentDateTime = None
            timeDiff = 0
            
            pipeline = [
                {"$match": {
                    "imei": imei,
                    "gps": "A",
                    "date_time": {
                        "$gte": start_of_day,
                        "$lt": end_of_day
                    },
                }},
                {"$sort": {"date_time": 1}},
                {"$project": {"date_time": 1, "speed": 1, "ignition": 1}},
            ]
            print(f'{imei} - Using AIS140 data for stoppage calculation')
            results = list(atlanta_collection.aggregate(pipeline))

            if not results:
                pipeline = [
                    {"$match": {
                        "imei": imei,
                        "gps.gpsStatus": 1,
                        "gps.timestamp": {"$gte": start_of_day, "$lt": end_of_day},
                    }},
                    {"$sort": {"gps.timestamp": 1}},
                    {"$project": {"gps.timestamp": 1, "telemetry.speed": 1, "telemetry.ignition": 1}},
                ]

                results = list(atlantaAis140_collection.aggregate(pipeline))
                
                if not results:
                    continue
                for result in results:
                    if result['telemetry']['ignition'] != 0:
                        currentDateTime = result['gps']['timestamp']
                        continue
                    
                    if not currentDateTime:
                        currentDateTime = result['gps']['timestamp']
                    else:
                        timeDiff = timeDiff + int((result['gps']['timestamp'] - currentDateTime).total_seconds() * 1000)
                        currentDateTime = result['gps']['timestamp']
                        print(timeDiff)
                
                resultItems.append({
                    "imei": imei,
                    "stoppage_time_delta": int(timeDiff),
                    "stoppage_time_str": format_seconds(timeDiff)
                })

                currentDateTime = None
                timeDiff = 0
                continue

            for result in results:
                if result['ignition'] != "0":
                    continue

                if not currentDateTime:
                    currentDateTime = result['date_time']
                else:
                    timeDiff = timeDiff + int((result['date_time'] - currentDateTime).total_seconds() * 1000)
                    currentDateTime = result['date_time']
                    print(timeDiff)
                
            resultItems.append({
                "imei": imei,
                "stoppage_time_delta": int(timeDiff),
                "stoppage_time_str": format_seconds(timeDiff),
            })
            currentDateTime = None
            timeDiff = 0

        return resultItems
    except Exception as e:
        print(f"Error calculating stoppage times: {e}")
        return []

def getVehicleDistances(imei):
    try:
        now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        ist_start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        ist_end_of_day = ist_start_of_day + timedelta(days=1)
        
        start_of_day = ist_start_of_day.astimezone(timezone.utc)
        end_of_day = ist_end_of_day.astimezone(timezone.utc)

        distances_atlanta = getDistanceBasedOnTime(imei, start_of_day, end_of_day)

        distances = {}
        
        for distance in distances_atlanta:
            first_odometer = float(distance.get('first_odometer', 0) or 0)
            last_odometer = float(distance.get('last_odometer', 0) or 0)
            distance_traveled = last_odometer - first_odometer
            distances[distance['imei']] = distance_traveled if distance_traveled >= 0 else 0
            
        return distances
    except Exception as e:
        print(f"Error fetching distances: {e}")
        return {}

def build_vehicle_data(inventory_data, distances, stoppage_times, statuses, imei_list, missingImeis):
    vehicles = []

    inventory_lookup = {v.get('IMEI'): v for v in inventory_data}
    stoppage_lookup = {item['imei']: item for item in stoppage_times}
    status_lookup = {item['imei']: item for item in statuses}

    vehicleData = list(atlantaLatest_collection.find({"_id": {"$in": imei_list}}, {"timestamp": 0}))

    for imei in imei_list:
        data = atlantaAis140Latest_collection.find_one({"_id": imei})
        if data:
            vehicleData.append(atlantaAis140ToFront(data))
    
    for vehicle in vehicleData:
        imei = vehicle.get('imei')
        if not imei:
            continue

        inventory = inventory_lookup.get(imei, {})
        vehicle["LicensePlateNumber"] = inventory.get('LicensePlateNumber', 'Unknown')
        vehicle["VehicleType"] = inventory.get('VehicleType', 'Unknown')
        vehicle["slowSpeed"] =  float(inventory.get('slowSpeed', "0") or 40.0)
        vehicle["normalSpeed"] = float(inventory.get('normalSpeed', "0") or 60.0)

        vehicle["distance"] = round(distances.get(imei, 0), 2)

        stoppage_time_item = stoppage_lookup.get(imei, {})
        vehicle['stoppage_time'] = stoppage_time_item.get('stoppage_time_str', '0 seconds')
        vehicle['stoppage_time_delta'] = stoppage_time_item.get('stoppage_time_delta', 0)

        if imei not in missingImeis:
            status_item = status_lookup.get(imei, {})
            vehicle['status'] = status_item.get('status', 'unnown')
            vehicle['status_time_str'] = status_item.get('status_time_str', '0 seconds')
            vehicle['status_time_delta'] = status_item.get('status_time_delta', 0)
            date = vehicle.get('date')
            time = vehicle.get('time')
            ignition = vehicle.get('ignition')
            speed = vehicle.get('speed')
            gsm_sig = vehicle.get('gsm_sig')
            vehicle['date'] = status_item.get('date', date)
            vehicle['time'] = status_item.get('time', time)
            vehicle['ignition'] = status_item.get('ignition', ignition)
            vehicle['speed'] = status_item.get('speed', speed)
            vehicle['gsm_sig'] = status_item.get('gsm_sig', gsm_sig)
        else:
            vehicle['status'] = 'offline'
            now = datetime.now(timezone.utc).timestamp() * 1000
            date_timeMs = vehicle.get('date_time').timestamp() * 1000
            status_time_delta = (now - date_timeMs)
            status_time_str = format_seconds(status_time_delta)
            vehicle['status_time_str'] = status_time_str
            vehicle['status_time_delta'] = status_time_delta
        
        vehicles.append(vehicle)
    return vehicles

def get_vehicles():
    try:

        inventory_data = vehicle_inventory_collection.find()
        
        imei_list = list(inventory_data.distinct("IMEI"))
        
        if not imei_list:
            return

        distances = getVehicleDistances(imei_list)
        if not distances:
            return
        
        stoppage_times = getStopTimeToday(imei_list)
        if not stoppage_times:
            return

        statuses, missingImeis = getVehicleStatus(imei_list)
        if not statuses:
            return

        vehicles = build_vehicle_data(inventory_data, distances, stoppage_times, statuses, imei_list, missingImeis)
        
        if not vehicles:
            return
        
        for vehicle in vehicles:
            vehicle.pop('_id', None)
            lat = vehicle.get('latitude')
            lng = vehicle.get('longitude')
            vehicle['location'] = geocodeInternal(lat, lng)
            
            live_map_collection.update_one(
                {"_id": vehicle.get('imei')},
                {"$set": vehicle},
                upsert=True
            )
            
    except Exception as e:
        print("Error fetching vehicle data:", e)
        
if __name__ == '__main__':
    while True:
        get_vehicles()
        time.sleep(120)  # Wait for 60 seconds before running the function again