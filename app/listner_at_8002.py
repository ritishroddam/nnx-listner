import asyncio
import googlemaps
import socketio

from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict, Any, List
from math import atan2, degrees, radians, sin, cos
from geopy.distance import geodesic
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import InsertOne, ReplaceOne, ASCENDING, DESCENDING

from alerts import dataToAlertParser
from parser import handle_can

# -----------------------
# Config
# -----------------------
HOST = "0.0.0.0"
PORT = 8002
READ_TIMEOUT = 300
DIRECTIONS = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
BEARING_DEGREES = 360 / len(DIRECTIONS)
GMAPS = googlemaps.Client(key="AIzaSyCHlZGVWKK4ibhGfF__nv9B55VxCc-US84")

# Tune bulk flush for your infra (throughput vs latency)
BULK_MAX_DOCS = 500
BULK_MAX_LATENCY_MS = 200

last_emit_time = {}
rawLogList = []
rawLogImeiLiscenceMap = {}

MONGO_URI = "mongodb+srv://doadmin:U6bOV204y9r75Iz3@private-db-mongodb-blr1-96186-4485159f.mongo.ondigitalocean.com/admin?tls=true&authSource=admin&replicaSet=db-mongodb-blr1-96186"
if not MONGO_URI:
    raise RuntimeError("MONGO_URI env var is required")

DB_NAME = "nnx"
COL_LOC = "atlantaAis140"            # structured history (location/events/health/emergency)
COL_RAW = "rawLogAtlantaAis140"      # raw packets (audit; TTL index applied)
COL_LATEST = "atlantaAis140_latest"  # one doc per IMEI (CP only, full doc)
COL_HEALTH = "atlantaAis140_health"
COL_GEOCODED = "geocoded_address"
COL_RAW_SUBSCRIPTIONS = "raw_log_subscriptions"
COL_VEHICLE_INVENTORY = "vehicle_inventory"
COL_VEHICLE_ODOMETER = "vehicle_odometer"
# -----------------------
# Mongo (Motor)
# -----------------------
mongo_client: AsyncIOMotorClient = AsyncIOMotorClient(MONGO_URI, tz_aware=True)
db = mongo_client[DB_NAME]
loc_coll = db[COL_LOC]
raw_coll = db[COL_RAW]
latest_coll = db[COL_LATEST]
health_coll = db[COL_HEALTH]
rawLogSubscriptions = db[COL_RAW_SUBSCRIPTIONS]
geocode_coll = db[COL_GEOCODED]
vehicle_invy_coll = db[COL_VEHICLE_INVENTORY]
vehicle_odometer_coll = db[COL_VEHICLE_ODOMETER]

# -----------------------
# Batching queues (backpressure)
# -----------------------
loc_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=50_000)
raw_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=50_000)
latest_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=50_000)
health_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=50_000)

# -----------------------
# Helpers
# -----------------------

def _should_emit(imei, date_time):
    
    if imei not in last_emit_time or date_time - last_emit_time[imei] > timedelta(seconds=1):
        last_emit_time[imei] = date_time
        return True
    return False

def _ensure_socket_connection():
    if not sio.connected:
        print(f"[{datetime.now()}] ! Socket.IO disconnected, attempting to reconnect...")
        try:
            sio.disconnect()
        except Exception:
            pass
        try:
            sio.connect(server_url, transports = ['websocket'])
            print(f"[{datetime.now()}] * Socket.IO reconnected")
        except Exception as e:
            print(f"[{datetime.now()}] ! Socket.IO reconnection failed: {e}")

def _validate_coordinates(lat, lng):
    if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
        raise ValueError(f"Invalid coordinates {lat} and {lng}")

def _calculate_bearing(coord1, coord2):
    lat1, lon1 = radians(coord1[0]), radians(coord1[1])
    lat2, lon2 = radians(coord2[0]), radians(coord2[1])
    d_lon = lon2 - lon1
    x = sin(d_lon) * cos(lat2)
    y = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(d_lon)
    bearing = (degrees(atan2(x, y)) + 360) % 360
    return DIRECTIONS[int(((bearing + (BEARING_DEGREES/2)) % 360) // BEARING_DEGREES)]

async def _geocodeInternal(lat,lng):
    try:
        lat = float(lat)
        lng = float(lng)
        _validate_coordinates(lat, lng)
    except(ValueError, TypeError) as e:
        print(f"Invalid input: {str(e)}")
        return "Invalid coordinates"

    try:
        nearby_entries = geocode_coll.find({
            "lat": {"$gte": lat - 0.0045, "$lte": lat + 0.0045},
            "lng": {"$gte": lng - 0.0045, "$lte": lng + 0.0045}
        })
        
        nearest_entry = None
        min_distance = 0.5 
        
        async for entry in nearby_entries:
            saved_coord = (entry['lat'], entry['lng'])
            current_coord = (lat, lng)
            distance = geodesic(saved_coord, current_coord).km
            
            if distance <= min_distance:
                nearest_entry = entry
                min_distance = distance

        if nearest_entry:
            saved_coord = (nearest_entry['lat'], nearest_entry['lng'])
            current_coord = (lat, lng)
            bearing = _calculate_bearing(saved_coord, current_coord)
            
            return (f"{min_distance:.2f} km {bearing} from {nearest_entry['address']}"
                      if min_distance > 0 else nearest_entry['address'])

        reverse_geocode_result = GMAPS.reverse_geocode((lat, lng))
        if not reverse_geocode_result:
            print("Geocoding API failed")
            return "Address unavailable"

        address = reverse_geocode_result[0]['formatted_address']

        await geocode_coll.insert_one({
            'lat': lat,
            'lng': lng,
            'address': address
        })

        return address

    except Exception as e:
        print(f"Geocoding error: {str(e)}")
        return "Error retrieving address"

def _pad_left(s: Optional[str], length: int) -> Optional[str]:
    if s is None:
        return None
    s = s.strip()
    if len(s) >= length:
        return s
    return s.zfill(length)

def _ddmmyyyy_hhmmss_to_utc(date_str: Optional[str], time_str: Optional[str]) -> Optional[datetime]:
    """
    AIS 140 gives GPS time in UTC (ddmmyyyy, hhmmss). Some devices omit leading zeros.
    We left-pad to 8/6 chars when needed.
    """
    d = _pad_left(date_str or "", 8)
    t = _pad_left(time_str or "", 6)
    try:
        return datetime(
            int(d[4:8]), int(d[2:4]), int(d[0:2]),
            int(t[0:2]), int(t[2:4]), int(t[4:6]),
            tzinfo=timezone.utc
        )
    except Exception:
        return None

def _to_float(val: Optional[str]) -> Optional[float]:
    try:
        return float(val) if val not in (None, "") else None
    except Exception:
        return None

def _to_int(val: Optional[str]) -> Optional[int]:
    try:
        return int(val) if val not in (None, "") else None
    except Exception:
        return None

def _ns_ew_to_signed(lat: Optional[float], ns: Optional[str],
                     lon: Optional[float], ew: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    if lat is not None and ns:
        lat = abs(lat) if ns.upper() == "N" else -abs(lat)
    if lon is not None and ew:
        lon = abs(lon) if ew.upper() == "E" else -abs(lon)
    return lat, lon

def _parse_neighbors(neigh_fields: List[str]) -> List[Dict[str, Any]]:
    """
    Neighboring cells appear as repeating triplets: [nmr, lac, cellId]
    In the example, neighbors start at index 34 and run through index 45 (inclusive),
    i.e., slice fields[35:46].
    """
    res: List[Dict[str, Any]] = []
    for i in range(0, len(neigh_fields), 3):
        try:
            gsmSignal = _to_int(neigh_fields[i])
            lac = neigh_fields[i + 1]
            cell_id = neigh_fields[i + 2]
            if cell_id:
                res.append({"cellId": cell_id, "lac": lac, "nmr": gsmSignal})
        except Exception:
            continue
    return res

def _convert_date_time_emit(date):
    if not date:
        now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        return now.strftime("%d%m%y"), now.strftime("%H%M%S")
    # Convert UTC datetime to IST (UTC+5:30)
    ist = date.astimezone(timezone(timedelta(hours=5, minutes=30)))
    return ist.strftime("%d%m%y"), ist.strftime("%H%M%S")

# -----------------------
# Packet parsing
# -----------------------

async def parse_for_emit(parsedData):
    date, time = _convert_date_time_emit(parsedData.get("timestamp"))
    inventoryData = await vehicle_invy_coll.find_one({"IMEI": parsedData.get("imei")})
    if inventoryData:
        licensePlateNumber = inventoryData.get("LicensePlateNumber", "Unknown")
        vehicleType = inventoryData.get("VehicleType", "Unknown")
        slowSpeed = float(inventoryData.get("slowSpeed", "40.0"))
        normalSpeed = float(inventoryData.get("normalSpeed", "60.0"))
    else:
        licensePlateNumber = "Unknown"
        vehicleType = "Unknown"
        slowSpeed = 40.0
        normalSpeed = 60.0
    
    if parsedData.get("gps", {}).get("gpsStatus") ==  1:
        address = await _geocodeInternal(parsedData.get("gps", {}).get("lat"), parsedData.get("gps", {}).get("lon"))
    else:
        address = "Address unavailable"
        
    gps = parsedData.get("gps", {}) or {}
    telemetry = parsedData.get("telemetry", {}) or {}
    network = parsedData.get("network", {}) or {}
    packet = parsedData.get("packet", {}) or {}
    packet_id = str(packet.get("id", ""))
    
    json_data = {
        "imei": parsedData.get("imei"),
        "LicensePlateNumber": licensePlateNumber,
        "VehicleType": vehicleType,
        "speed": telemetry.get("speed"),
        "latitude": gps.get("lat"),
        "dir1": gps.get("latDir"),
        "longitude": gps.get("lon"),
        "dir2": gps.get("lonDir"),
        "date": date,
        "time": time,
        "course": gps.get("heading"),
        "ignition": telemetry.get("ignition"),
        "gsm_sig": network.get("gsmSignal"),
        "sos": telemetry.get("emergencyStatus"),
        "odometer": telemetry.get("odometer"),
        "main_power": telemetry.get("mainPower"),
        "harsh_speed": "1" if packet_id == "14" else "0",
        "harsh_break": "1" if packet_id == "13" else "0",
        "adc_voltage": telemetry.get("mainBatteryVoltage"),
        "internal_bat": telemetry.get("internalBatteryVoltage"),
        "mobCountryCode": network.get("mcc"),
        "mobNetworkCode": network.get("mnc"),
        "localAreaCode": network.get("lac"),
        "cellid": network.get("cellId"),
        "gps": "A" if gps.get("gpsStatus") == 1 else "V",
        "status": "01" if packet.get("status") == "L" else "03",
        "date_time": str(gps.get("timestamp").astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M:%S")),
        "timestamp": str(parsedData.get("timestamp").astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M:%S")),
        "address": address,
        "normalSpeed": normalSpeed,
        "slowSpeed": slowSpeed,
    }
    return json_data

async def extract_can_frames(raw_packet: str):
    """
    Extract CAN frames from AIS140 extended packet.
    Returns list of {id,data}
    """

    if "02|" not in raw_packet:
        return []

    try:
        # Step 1 — isolate CAN portion (remove checksum)
        print(f"[DEBUG] Extracting CAN frames from raw packet: {raw_packet}")
        can_part = raw_packet.split("02|", 1)[1]
        can_part = can_part.split("*")[0]

        # Step 2 — split frames by |
        parts = can_part.split("|")

        # parts[0] is event timestamp like 00:00 → ignore
        parts = parts[1:]

        frames = []

        for part in parts:
            if ":" not in part:
                continue

            can_id, can_data = part.split(":", 1)

            # sanity checks
            if len(can_id) != 8:
                continue
            if len(can_data) != 16:
                continue

            frames.append({
                "id": can_id.upper(),
                "data": can_data.upper()
            })

        return frames

    except Exception as e:
        print(f"[ERROR] CAN extraction error: {e}")
        return []


async def parse_can_packet(g: callable, vrn: str, imei: str, date_raw: str, time_raw: str, lat: float, lon: float, lat_dir: str, lon_dir: str, ts: datetime) -> Dict[str, Any]:
    try:
        raw_can_data = g(41)
        can_frames = await extract_can_frames(raw_can_data)
        if not can_frames:
            return {}
        
        canData = await handle_can(imei, can_frames, ts)

        if "odometer_km" in canData and _to_int(g(23)) == 1:
            new_odometer = round(_to_float(canData["odometer_km"]), 2)
        else:
            vehicle_odometer_data = await vehicle_odometer_coll.find_one({"imei": imei})
            odometer_history = vehicle_odometer_data.get("odometer", 0) if vehicle_odometer_data else 0
            odometer_current = _to_float(g(39))
            
            print(f"[DEBUG] Odometer values for IMEI {imei} - History: {odometer_history} km, Current: {odometer_current} m")

            new_odometer = round(odometer_history + (odometer_current / 1000) if odometer_current is not None else odometer_history, 2)
            
            print(f"[DEBUG] Calculated new odometer for IMEI {imei}: {new_odometer} km")

        await vehicle_odometer_coll.update_one(
            {"imei": imei},
            {"$set": {"odometer": new_odometer}},
            upsert=True
        )


        doc: Dict[str, Any] = {
            "type": "LOCATION",
            "imei": imei,
            "LicensePlateNumber": vrn if vrn not in ("", None) else None,
            "timestamp": datetime.now(timezone.utc),
            "vendor": g(2),
            "firmware": g(3),
            "packet": {
                "type": g(4),
                "id": g(5),
                "status": g(6),
                "frameNumber": _to_int(g(36)),
            },
            "gps": {
                "date": _pad_left(date_raw, 8),       # raw ddmmyyyy (zero-padded)
                "time": _pad_left(time_raw, 6),       # raw hhmmss (zero-padded)
                "timestamp": ts,                      # ISO UTC (can be None if malformed)
                "gpsStatus": _to_int(g(9)),
                "lat": lat,
                "lon": lon,
                "latDir": lat_dir or None,
                "lonDir": lon_dir or None,
                "heading": _to_float(g(17)),
                "numSatellites": _to_int(g(18)),
                "altitude": _to_float(g(19)),
                "pdop": _to_float(g(20)),
                "hdop": _to_float(g(21)),
            },
            "telemetry": {
                "speed": _to_float(g(16)),
                "ignition": _to_int(g(23)),
                "mainPower": _to_int(g(24)),
                "mainBatteryVoltage": _to_float(g(25)),
                "internalBatteryVoltage": _to_float(g(26)),
                "emergencyStatus": _to_int(g(27)),
                "tamper": g(28),
                "odometer": new_odometer,
            },
            "network": {
                "operator": g(22),
                "gsmSignal": _to_int(g(29)),
                "mcc": _to_int(g(30)),
                "mnc": _to_int(g(31)),
                "lac": g(32),
                "cellId": g(33),
            },
            "io": {
                "digitalInputs": g(34),
                "digitalOutputs": g(35),
                "analog1": _to_float(g(37)),
                "analog2": _to_float(g(38)),
            },
            "canData": canData,  # Placeholder for future CAN bus parsing (e.g. OBD2)
            "dataTypeIndicator": g(40),
        }
        return doc
    except Exception as e:
        print(f"[ERROR] CAN packet parsing error: {e}")
        return {}

async def parse_packet(raw: str) -> Dict[str, Any]:
    """
    Robust parsing for:
      - CP: Location/Event (Table-4/5 in AIS140)  -> saved to history + latest (latest=CP only)
      - HP: Health (Table-2)                      -> saved to history
      - EPB: Emergency (EMR/SEM)                  -> saved to history
    """
    pkt: Dict[str, Any] = {"ingestedAt": datetime.now(timezone.utc)}
    raw = raw.strip()
    if not raw.startswith("$"):
        pkt["type"] = "UNKNOWN"
        return pkt

    # Trim trailing '*' (checksum terminator visible in examples)
    
    raw_split1 = raw[:1]
    raw_split2 = raw[1:]
    
    raw = raw_split1 + ',' + raw_split2
    
    trimmed = raw[:-1] if raw.endswith("*") else raw
    parts = [p.strip() for p in trimmed.split(",")]
    if len(parts) < 2:
        pkt["type"] = "UNKNOWN"
        return pkt

    header = parts[1]
    pkt["header"] = header

    # Utility for safe index access
    def g(i: int) -> Optional[str]:
        return parts[i] if i < len(parts) else None

    # ---------- CP (Location) ----------
    if header == "Header" and (len(parts) >= 41):
        # Index map from your sample:
        # 0:$ 1:CP 2:Vendor 3:FW 4:Type 5:ID 6:Status 7:IMEI 8:VRN 9:Fix
        # 10:Date 11:Time 12:Lat 13:N/S 14:Lon 15:E/W 16:Speed 17:Heading 18:Sats 19:Alt
        # 20:PDOP 21:HDOP 22:Operator 23:Ign 24:MainPower 25:MainBattV 26:IntBattV
        # 27:Emergency 28:Tamper 29:GsmSig 30:MCC 31:MNC 32:LAC 33:CellId(serving)
        # 34..45: Neighbors (cellId,lac,ss) x4
        # 46:Odometer 47:DigitalInputs 48:FrameNo 49:Analog1 50:Analog2 51:DigitalOutputs 52:Checksum
        imei = g(7)
        vrn = g(8)
        date_raw = g(10) or ""
        time_raw = g(11) or ""

        ts = _ddmmyyyy_hhmmss_to_utc(date_raw, time_raw)
        lat = _to_float(g(12)); lon = _to_float(g(14))
        lat_dir = g(13) or ""; lon_dir = g(15) or ""
        lat, lon = _ns_ew_to_signed(lat, lat_dir, lon, lon_dir)
        
        if g(5) == "10":
            padded_date = _pad_left(date_raw, 8)
            ddmmyy = padded_date[:6] + padded_date[6:8] if len(padded_date) == 8 else padded_date[:6]
            packet = {
                'imei': imei,
                'date': ddmmyy,
                'time': _pad_left(time_raw, 6),
                'latitude': str(lat),
                'longitude': str(lon),
                'date_time': ts,
                'timestamp': datetime.now(timezone.utc),
            }
            
            try:
                db['sos_logs'].insert_one(packet)
            except Exception as e:
                print("Error logging SOS alert to MongoDB:", e)
        
        print(f"[DEBUG] Parsed packet with ID {g(5)} for IMEI {imei}")  
        if g(5) == "2000":
            doc = await parse_can_packet(g, vrn, imei, date_raw, time_raw, lat, lon, lat_dir, lon_dir, ts)
            return doc
        
        # neighbors slice inclusive of index 45 -> [35:46]
        neighbors = _parse_neighbors(parts[34:46])
        
        vehicle_odometer_data = await vehicle_odometer_coll.find_one({"imei": imei})
        odometer_history = vehicle_odometer_data.get("odometer", 0) if vehicle_odometer_data else 0
        odometer_current = _to_float(g(51))
        
        new_odometer = round(odometer_history + (odometer_current / 1000) if odometer_current is not None else odometer_history, 2)
        
        await vehicle_odometer_coll.update_one(
            {"imei": imei},
            {"$set": {"odometer": new_odometer}},
            upsert=True
        )
        
        doc: Dict[str, Any] = {
            "type": "LOCATION",
            "imei": imei,
            "LicensePlateNumber": vrn if vrn not in ("", None) else None,
            "timestamp": datetime.now(timezone.utc),
            "vendor": g(2),
            "firmware": g(3),
            "packet": {
                "type": g(4),
                "id": g(5),
                "status": g(6),
                "frameNumber": _to_int(g(48)),
            },
            "gps": {
                "date": _pad_left(date_raw, 8),       # raw ddmmyyyy (zero-padded)
                "time": _pad_left(time_raw, 6),       # raw hhmmss (zero-padded)
                "timestamp": ts,                      # ISO UTC (can be None if malformed)
                "gpsStatus": _to_int(g(9)),
                "lat": lat,
                "lon": lon,
                "latDir": lat_dir or None,
                "lonDir": lon_dir or None,
                "heading": _to_float(g(17)),
                "numSatellites": _to_int(g(18)),
                "altitude": _to_float(g(19)),
                "pdop": _to_float(g(20)),
                "hdop": _to_float(g(21)),
            },
            "telemetry": {
                "speed": _to_float(g(16)),
                "ignition": _to_int(g(23)),
                "mainPower": _to_int(g(24)),
                "mainBatteryVoltage": _to_float(g(25)),
                "internalBatteryVoltage": _to_float(g(26)),
                "emergencyStatus": _to_int(g(27)),
                "tamper": g(28),
                "odometer": new_odometer,
            },
            "network": {
                "operator": g(22),
                "gsmSignal": _to_int(g(29)),
                "mcc": _to_int(g(30)),
                "mnc": _to_int(g(31)),
                "lac": g(32),
                "cellId": g(33),
                "neighbors": neighbors,
            },
            "io": {
                "digitalInputs": g(46),
                "digitalOutputs": g(47),
                "analog1": _to_float(g(49)),
                "analog2": _to_float(g(50)),
            },
            "checksum": (g(52) or "").replace("*", "").strip(),
        }
        return doc

    # ---------- HP (Health) ----------
    if header == "Header" and len(parts) == 13:
        vendor = g(2); firmware = g(3)
        imei = g(4)
        doc = {
            "type": "HEALTH",
            "imei": imei,
            "timestamp": datetime.now(timezone.utc),
            "vendor": vendor,
            "firmware": firmware,
            "health": {
                "batteryLevel": _to_float(g(5)),
                "lowBatteryThresh": _to_float(g(6)),
                "memoryUsage": _to_float(g(7)),
                "updateRate": {"ignOnSec": _to_float(g(8)), "ignOffSec": _to_float(g(9))},
                "digitalInputs": g(10),
                "analogA1": _to_float(g(11)),
                "analogA2": _to_float(g(12)),
            },
        }
        return doc

    # ---------- EPB (Emergency) ----------
    if header == "EPB":
        msg_type = g(2)     # EMR/SEM
        imei = g(3)
        pkt_type = g(4)     # NM/SP
        dtts = g(5) or ""   # ddmmyyyyhhmmss
        gps_valid = g(6)
        lat = _to_float(g(7)); lat_dir = g(8) or ""
        lon = _to_float(g(9)); lon_dir = g(10) or ""
        heading = _to_float(g(11))
        speed = _to_float(g(12))
        vrn = g(15)

        ts = None
        if len(dtts) >= 14:
            try:
                ts = datetime(
                    int(dtts[4:8]), int(dtts[2:4]), int(dtts[0:2]),
                    int(dtts[8:10]), int(dtts[10:12]), int(dtts[12:14]),
                    tzinfo=timezone.utc
                )
            except Exception:
                ts = None

        lat, lon = _ns_ew_to_signed(lat, lat_dir, lon, lon_dir)

        doc = {
            "type": "EMERGENCY",
            "imei": imei,
            "LicensePlateNumber": vrn if vrn not in ("", None) else None,
            "timestamp": ts or datetime.now(timezone.utc),
            "emergency": {"messageType": msg_type, "packetType": pkt_type, "gpsValid": gps_valid},
            "gps": {"lat": lat, "lon": lon},
            "telemetry": {"speed": speed, "heading": heading},
        }
        return doc

    # Unknown packet (e.g. vendor login)
    pkt["type"] = "UNKNOWN"
    return pkt

# -----------------------
# Bulk writer workers
# -----------------------

async def _bulk_insert_worker(coll, q: "asyncio.Queue[Dict[str, Any]]", name: str):
    """
    Generic bulk insert worker: consumes docs from q and bulk inserts into coll.
    Time- and size-based flush.
    """
    ops: List[InsertOne] = []
    loop = asyncio.get_event_loop()
    last = loop.time()

    async def flush():
        nonlocal ops, last
        if not ops:
            return
        try:
            await coll.bulk_write(ops, ordered=False)
        except Exception:
            # fallback: single inserts
            for op in ops:
                try:
                    await coll.insert_one(op._doc)  # type: ignore
                except Exception:
                    pass
        ops = []
        last = loop.time()

    while True:
        try:
            timeout = BULK_MAX_LATENCY_MS / 1000.0
            try:
                doc = await asyncio.wait_for(q.get(), timeout=timeout)
                ops.append(InsertOne(doc))
                if len(ops) >= BULK_MAX_DOCS:
                    await flush()
            except asyncio.TimeoutError:
                await flush()
        except asyncio.CancelledError:
            if ops:
                try:
                    await coll.bulk_write(ops, ordered=False)
                except Exception:
                    for op in ops:
                        try:
                            await coll.insert_one(op._doc)  # type: ignore
                        except Exception:
                            pass
            break

async def _bulk_latest_upsert_worker(coll, q: "asyncio.Queue[Dict[str, Any]]"):
    """
    Bulk upsert into `atlantaAis140_latest` with IMEI as `_id`.
    IMPORTANT: Only CP (type='LOCATION') is written to latest, and with FULL CP doc.
    """
    ops: List[ReplaceOne] = []
    loop = asyncio.get_event_loop()
    last = loop.time()

    async def flush():
        nonlocal ops, last
        if not ops:
            return
        try:
            await coll.bulk_write(ops, ordered=False)
        except Exception:
            for op in ops:
                try:
                    await coll.replace_one(op._filter, op._replacement, upsert=True)  # type: ignore
                except Exception:
                    pass
        ops = []
        last = loop.time()

    while True:
        try:
            timeout = BULK_MAX_LATENCY_MS / 1000.0
            try:
                doc = await asyncio.wait_for(q.get(), timeout=timeout)
                if doc.get("type") != "LOCATION":
                    continue
                gps_info = doc.get("gps") or {}
                if gps_info.get("gpsStatus") != 1:
                    continue
                imei = doc.get("imei")
                if not imei:
                    continue
                # full CP doc in latest; add _id = IMEI
                latest_doc = dict(doc)
                latest_doc["_id"] = imei
                ops.append(ReplaceOne({"_id": imei}, latest_doc, upsert=True))
                if len(ops) >= BULK_MAX_DOCS:
                    await flush()
            except asyncio.TimeoutError:
                await flush()
        except asyncio.CancelledError:
            if ops:
                try:
                    await coll.bulk_write(ops, ordered=False)
                except Exception:
                    for op in ops:
                        try:
                            await coll.replace_one(op._filter, op._replacement, upsert=True)  # type: ignore
                        except Exception:
                            pass
            break

# -----------------------
# TCP client handler
# -----------------------

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    print(f"[{datetime.now()}] + Connect {addr}")
    buffer = bytearray()

    try:
        while True:
            chunk = await asyncio.wait_for(reader.read(4096), timeout=READ_TIMEOUT)
            print(f"[{datetime.now()}] Read {chunk} bytes from {addr}")
            if not chunk:
                print(f"[{datetime.now()}] - Disconnect {addr}")
                break
            buffer.extend(chunk)

            # Parse multiple packets in the buffer.
            # AIS140 packets start with '$' and end with '*'
            while True:
                start = buffer.find(b"$")
                if start == -1:
                    if len(buffer) > 1024 * 1024:
                        buffer.clear()
                    break
                end = buffer.find(b"\r\n", start + 2)
                if end == -1:
                    if start > 0:
                        del buffer[:start]  # discard leading junk
                    break

                pkt_bytes = buffer[start:end + 2]  # include '*'
                del buffer[:end + 2]

                # Decode robustly
                try:
                    raw = pkt_bytes.decode("utf-8", errors="replace")
                except Exception:
                    raw = pkt_bytes.decode("latin-1", errors="replace")
                    
                raw = raw.encode('unicode_escape').decode('ascii')

                # Parse first (so raw log gets IMEI/VRN)
                parsed = await parse_packet(raw)

                # Enqueue raw (audit) -> include IMEI + VRN, and NO device address
                if parsed.get("imei") in rawLogList:
                    await raw_queue.put({
                        "imei": parsed.get("imei"),
                        "LicensePlateNumber": rawLogImeiLiscenceMap.get(parsed.get("imei")),
                        "raw_data": raw,
                        "timestamp": datetime.now(timezone.utc),
                    })

                # Enqueue structured into history
                ptype = parsed.get("type")
                if ptype == "LOCATION":
                    # Mirror gps.timestamp into top-level timestamp if available
                    # (Already done for CP; keep as-is for others)
                    await loc_queue.put(parsed)
                    # Only CP -> latest
                    if ptype == "LOCATION":
                        await latest_queue.put(parsed)

                    if parsed.get("gps", {}).get("gpsStatus") ==  0:
                        continue
                    
                    emit_data = await parse_for_emit(parsed)
                    await dataToAlertParser(emit_data)
                    
                    
                    if _should_emit(parsed.get("imei"), parsed.get("gps", {}).get("timestamp")):
                        _ensure_socket_connection()
                        if sio.connected:
                            try:
                                print("[DEBUG] sending Data of AIS140 Device for alerts")
                                if parsed.get("gps", {}).get("gpsStatus") ==  1:
                                    sio.emit('vehicle_live_update', emit_data)
                                    sio.emit('vehicle_update', emit_data)
                            except Exception as e:
                                print(f"[{datetime.now()}] ! Socket.IO emit error: {e}")
                        else:
                            print(f"[{datetime.now()}] ! Socket.IO not connected, skipping emit")

                elif ptype == "HEALTH":
                    await health_queue.put(parsed)


                # ACKs (only if device requires; disabled by default)
                # if ptype == "EMERGENCY" and parsed.get("imei"):
                # if parsed.get("imei"):
                #     ack = f"$123,${parsed['imei']},aquila123,diag,100*"
                #     writer.write(ack.encode())
                #     await writer.drain()

    except asyncio.TimeoutError:
        print(f"[{datetime.now()} ERROR] Timeout {addr} after {READ_TIMEOUT}s")
    except Exception as e:
        print(f"[{datetime.now()} ERROR] in handle_client {addr}: {e}")
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

# -----------------------
# Indexes & startup
# -----------------------

async def lastEmitInitial():
    results = latest_coll.find({}, {"_id": 1, "timestamp": 1})
    async for result in results:
        last_emit_time[result['_id']] = result.get('timestamp')

async def update_raw_log_list():
    while True:
        try: 
            results = rawLogSubscriptions.find()
            rawLogList.clear()
            rawLogImeiLiscenceMap.clear()
            async for result in results:
                imei = result.get('IMEI')
                if imei:
                    rawLogList.append(imei)
                    rawLogImeiLiscenceMap[imei] = result.get('LicensePlateNumber', 'Unknown')
        except Exception as e:
            print(f"[{datetime.now()} ERROR] updating raw log list: {e}")
        await asyncio.sleep(300)


async def ensure_indexes():
    # History queries
    await loc_coll.create_index([("imei", ASCENDING), ("timestamp", DESCENDING)])
    await loc_coll.create_index([("imei", ASCENDING), ("gps.timestamp", DESCENDING)])
    await loc_coll.create_index([("imei", ASCENDING),("gps.timestamp", ASCENDING),],unique=True,name="uniq_imei_gps_timestamp")
    await loc_coll.create_index([("LicensePlateNumber", ASCENDING), ("timestamp", DESCENDING)])
    await loc_coll.create_index([("LicensePlateNumber", ASCENDING), ("gps.timestamp", DESCENDING)])
    await loc_coll.create_index([("timestamp", DESCENDING)])
    await loc_coll.create_index([("gps.timestamp", DESCENDING)])

    # Latest: _id is IMEI (implicit); add ts index for dashboards
    await latest_coll.create_index([("timestamp", DESCENDING)])
    await latest_coll.create_index([("gps.timestamp", DESCENDING)])

    # Raw logs TTL (30 days) on 'timestamp'
    try:
        await raw_coll.create_index("timestamp", expireAfterSeconds=30 * 24 * 3600)
    except Exception:
        # If index exists with different TTL, you'd need to drop/recreate manually.
        pass

async def main():
    global sio, server_url
    
    sio = socketio.Client()
    
    await ensure_indexes()
    await lastEmitInitial()

    server_url = "https://cordonnx.com"
    
    try:
        sio.connect(server_url, transports = ['websocket'])
        print(f"[{datetime.now()}] Connected to Socket.IO server at {server_url}")
    except Exception as e:
        print(f"[{datetime.now()}] ! Failed to connect to Socket.IO server: {e}")
        
    
    # Start workers
    loc_task = asyncio.create_task(_bulk_insert_worker(loc_coll, loc_queue, "loc"))
    raw_task = asyncio.create_task(_bulk_insert_worker(raw_coll, raw_queue, "raw"))
    latest_task = asyncio.create_task(_bulk_latest_upsert_worker(latest_coll, latest_queue))
    health_task = asyncio.create_task(_bulk_insert_worker(health_coll, health_queue, "health"))

    rawLogListTask = asyncio.create_task(update_raw_log_list())
    
    server = await asyncio.start_server(handle_client, HOST, PORT)
    print(f"[{datetime.now()}] Listening on {HOST}:{PORT}")
    async with server:
        try:
            await server.serve_forever()
        finally:
            # Graceful shutdown: flush remaining ops
            for t in (loc_task, raw_task, latest_task, rawLogListTask, health_task):
                t.cancel()
            await asyncio.gather(loc_task, raw_task, latest_task, rawLogListTask, health_task, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
